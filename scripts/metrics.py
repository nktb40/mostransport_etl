import json
import os
import glob
import numpy
import csv
import turf
import multiprocessing
from datetime import datetime
import shapely
from shapely.geometry import *
from functools import reduce
from datetime import datetime

# Переменная для хранения врменных результатов работы в многопоточном режиме
results = []

	
# Функция поиска объектов внутри изохрона
def get_objects_inside_info(source,geometry):
	# Список объектов, которые попали в изохрон
	in_polygon = []

	# Проходимся по списку объектов и проверяем гаходится ли он в границах
	multipoint_list = []
	for point in source:
		point_geo = shape(point['geometry']) if len(point['geometry']['coordinates']) > 0 else None
		if point_geo != None:
			multipoint_list.append(point_geo)

	mp = MultiPoint(multipoint_list)
	res = mp.intersection(geometry)

	if res.is_empty == False:
		if res.geom_type == 'Point':
			res_list = [[res.x,res.y]]
		else:
			res_list = list(map(lambda p: [p.x,p.y],list(res)))

		for point in source:
			point_geo = point['geometry']['coordinates']
			if len(point_geo) > 0:
				if point_geo in res_list:
					in_polygon.append(point)

	# считает кол-во точек, которые попали в изохрон
	points_cnt = len(in_polygon)

	# считаем кол-во людей, которые связаны с этими точками
	population = reduce(lambda x,y: x+y, map(lambda x: x['properties']['population'],in_polygon),0)

	return {"cnt":points_cnt, "population":population}


# Функция генерации метрик для изохронов
def generate_isohrone_metrics(isochrones, profile, part):
	# Список расчитанных метрик
	#{"metric_code":, "isochrone_code": , "metric_value": }
	metrics = []

	try:
		# Загружаем источники данных
		src_list = ['houses']
		sources = []
		for src in src_list:
			src_path = "../out/"+src+"/"+src+".geojson"
			if os.path.exists(src_path) == True:
				with open(src_path,"r") as file:
					sources.append({"name":src, "data":json.load(file)['features']})

		# Кол-во изохронов на входе для отслеживания прогресса
		iso_len = len(isochrones)

		# Выполняем расчёт метрик для каждого изохрона
		for i, row in enumerate(isochrones):
			iso_id = row['id']

			print("Metrics. "+profile+". Part: "+str(part)+". Row: "+str(i+1)+" from: "+str(iso_len)+". Last: "+iso_id)

			# Геометрия изохрона
			geom = shape(json.loads(row['geometry']))

			# Если имеются пересечения в полигоне, то заменяем его на буфер вокруг него, чтобы убрать пересечения
			if geom.is_valid == False:
				geom = geom.buffer(0)

			# Считаем площадь изохрона
			isochrone_area = round(turf.calculateArea(geom)/1000000,2)

			# Добавляем метрику площади в список метрик
			metrics.append({"metric_code":"isochrone_area", "isochrone_code": iso_id, "metric_value": isochrone_area})
			
			# Считаем метрики по вхождению объектов внутрь изохрона
			for s in sources:
				# Считаем кол-во объектов из источника внутри изохрона и соотв. кол-во жителей
				info = get_objects_inside_info(s["data"],geom)
				print(info)
				# Добавляем метрику "Кол-во объектов" в список метрик
				if info["cnt"] > 0:			
					metrics.append({"metric_code":s["name"]+"_cnt", "isochrone_code": iso_id, "metric_value": info["cnt"]})

				# Добавляем метрику "Кол-во людей" в список метрик
				if info["population"] > 0:
					metrics.append({"metric_code":s["name"]+"_population", "isochrone_code": iso_id, "metric_value": info["population"]})

		return metrics

	except Exception as e:
		print('Caught exception in worker thread',part,'. Error:', e)
		raise e


# Запуск расчёта метрик в многопоточном режиме
def run_isochrone_metrics_in_threads(profile,threads_num):
	print("Start run_metrics_in_threads. Time:",datetime.now())
	global results
	results = []

	paths = []
	if profile == 'public_transport':
		paths = glob.glob("../out/isochrones/public_transport/*.csv")
	else:
		paths.append("../out/isochrones/isochrones_"+profile+".csv")

	for path_num, path in enumerate(paths):
		with open(path,"r") as file:
			isochrones = list(map(lambda x: x, csv.DictReader(file, delimiter=';')))

		chunks = numpy.array_split(isochrones,threads_num)
		pool = multiprocessing.Pool(processes=threads_num)

		for i in range(0,threads_num):
			print("isochrones Metrics for "+profile+" Part: "+str(i+1)+" len: "+str(len(chunks[i])))
			pool.apply_async(generate_isohrone_metrics, args=(chunks[i],profile,str(i+1)), callback=collect_results)
		pool.close()
		pool.join()

	# Записываем результат в файл
	os.system('mkdir ../out/metrics')

	with open('../out/metrics/metrics_'+profile+'.csv', 'w') as file_out:
		fieldnames = results[0].keys()
		writer = csv.DictWriter(file_out, fieldnames=fieldnames,delimiter=';')
		writer.writeheader()
		# Записываем строки
		for r in results:
			writer.writerow(r)

	print("Finish run_metrics_in_threads. Time:",datetime.now())

def collect_results(result):
    results.extend(result)

# ============ Метрики для остановок ==================#

# Функция расчёта метрик для остановок
def generate_station_metrics(default_interval):
	print("Start generate_station_metrics")
	metrics = []
	
	# Считаем "Пешеходная доступность" 
	metrics.extend(generate_station_accessibility())
	# Считаем "Достяжимые остановки" 
	metrics.extend(generate_reachable_stops())
	# Считаем "Площадь покрытия на ОТ" 
	metrics.extend(generate_public_coverage())
	# Считаем "Число жителей на рейс" 
	metrics.extend(generate_population_on_track(default_interval))
	
	# Записываем результат в файл
	with open("../out/metrics/station_metrics.csv","w") as file:
		fieldnames = metrics[0].keys()
		writer = csv.DictWriter(file, fieldnames=fieldnames,delimiter=';')
		writer.writeheader()
		for m in metrics:
			writer.writerow(m)

	print("Finish generate_station_metrics")

# Функция расчёта метрики "Пешеходная доступность" (accessibility)
def generate_station_accessibility():
	print("Start generate_station_accessibility")
	metrics = []
	with open("../out/metrics/metrics_walking.csv","r") as file:
		reader = csv.DictReader(file, delimiter=';')
		for row in reader:
			params = row['isochrone_code'].split('-') # параметры исходного изохрона
			if(row['metric_code'] == 'isochrone_area' and params[2]=='walking' and params[3]=='5'): # проверяем что изохрон пеший и размер 5 минут
				metric = {
							"metric_code":"accessibility",
							"station_id":params[1],
							"metric_value":round(float(row['metric_value'])*100/0.78,2)
						 }
				metrics.append(metric)
	
	print("Finish generate_station_accessibility")

	return metrics

# Функция расчёта метрики "Достяжимые остановки" (reachable_stops)
def generate_reachable_stops():
	print("Start generate_reachable_stops")
	# Загружаем список остановок
	with open("../out/stations/geojson/stations.geojson","r") as file:
		stations = json.load(file)['features']

	# Загружаем связь остановок и маршрутов
	with open("../out/route2stops/route2stops.csv","r") as file:
		reader = csv.DictReader(file, delimiter=';')
		route2stops = []
		for r2s in reader:
			route2stops.append(r2s)

	metrics = []

	# Считаем метрику для каждой остановки
	for s in stations:
		
		st_id = s['properties']['global_id']
		route_list = s['properties']['RouteNumbers'].split('; ')
		reach_stations = []
		
		for r in route_list:
			# Фильтруем связи остановок и маршрутов по номеру маршрута
			r2s = list(filter(lambda x: x['route_code'] == r, route2stops))
			# Находим номера рейсов
			tracks = list(set(map(lambda x: x['track_no'], r2s)))
			for track in tracks:
				# Получаем номер остановки на текущем треке
				stop_on_track = next((x for x in r2s if x['track_no'] == track and x['station_id'] == st_id), None)
				if stop_on_track != None:
					s_seq = stop_on_track['seq_no']
					# Фильтруем остановки после текущей
					r2s_after = list(filter(lambda x: x['track_no'] == track and x['seq_no'] > s_seq and x['station_id'] != st_id, r2s))
					# Получаем список ID остановок после текущей
					next_stations = list(map(lambda x: x['station_id'],r2s_after))
					# Добавляем остановки в список
					reach_stations.extend(next_stations)

		reachable_stops = len(list(set(reach_stations)))
		metric = {
					"metric_code":"reachable_stops",
					"station_id":st_id,
					"metric_value":reachable_stops
				 }
		metrics.append(metric)

	print("Finish generate_reachable_stops")

	return metrics

# Функция расчёта Площади покрытия на обществнном транспорте
def generate_public_coverage():
	print("Start generate_public_transport_coverage")
	metrics = []
	with open("../out/metrics/metrics_public_transport.csv","r") as file:
		reader = csv.DictReader(file, delimiter=';')
		for row in reader:
			params = row['isochrone_code'].split('-') # параметры исходного изохрона
			if(row['metric_code'] == 'isochrone_area' and params[2]=='public_transport' and params[3] == '30' and params[4] == '1'):
				
				if params[5] == '1':
					# Площадь покрытия с пересадками
					metric = {
						"metric_code":"public_coverage_chng",
						"station_id":params[1],
						"metric_value":row['metric_value']
					}
				else:
					# Площадь покрытия без пересадок
					metric = {
						"metric_code":"public_coverage",
						"station_id":params[1],
						"metric_value":row['metric_value']
					 }
				#print(metric)
				metrics.append(metric)
	#print(metrics)
	print("Finish generate_public_transport_coverage")

	return metrics

# Отношение Число жителей на рейс + Число рейсов в час
def generate_population_on_track(default_interval):
	print("Start generate_population_to_tracks_cnt")
	
	# Число рейсов в день
	avg_interval = default_interval
	tracks_per_route_cnt = 18*60/avg_interval-1 # 18 часов * 60 минут / средний интервал - 1 (первый рейс)
	metrics = []

	# Загружаем список остановок
	with open("../out/stations/geojson/stations.geojson","r") as file:
		stations = json.load(file)['features']
	
	with open("../out/metrics/metrics_walking.csv","r") as file:
		reader = csv.DictReader(file, delimiter=';')
		for row in reader:

			params = row['isochrone_code'].split('-') # параметры исходного изохрона
			station_id = params[1]

			station = next(x for x in stations if x['properties']['global_id'] == station_id)
			# Кол-во маршрутов, проходящих через остановку
			routes_cnt = len(station['properties']['RouteNumbers'].split('; '))
			# Кол-во рейсов на остановке
			tracks_cnt = tracks_per_route_cnt*routes_cnt

			if(row['metric_code'] == 'houses_population' and params[2]=='walking' and params[3]=='5'): # проверяем что изохрон пеший и размер 5 минут
				# Число жителей на рейс
				metric = {
							"metric_code":"population_per_track",
							"station_id":station_id,
							"metric_value":round(float(row['metric_value'])/tracks_cnt,2)
						 }
				metrics.append(metric)

				# Число рейсов в час
				metric = {
							"metric_code":"tracks_per_hour",
							"station_id":station_id,
							"metric_value":round(60/avg_interval*routes_cnt)
						 }
				metrics.append(metric)
	
	print("Finish generate_population_to_tracks_cnt")

	return metrics

# # ============ Метрики для маршрутов ==================#
# Интервал
# Длина
# Стоимость
# Прямолинейность
# Площадь покрытия
# Кол-во жителей
# Кол-во домов
# Кол-во рейсов
# Число жителей на рейс

def generate_route_metrics(default_interval):
	print("Start generate_route_metrics")

	# Загружаем список с маршрутами
	with open("../out/routes/geojson/routes.geojson","r") as file:
		routes = json.load(file)['features']

	# Загружаем список с алтернативными маршрутами (прямыми)
	with open("../out/alternative_routes/alternative_routes.geojson","r") as file:
		alter_routes = json.load(file)['features']

	# Загружаем метрики изохронов маршрутов
	with open("../out/metrics/metrics_route_cover.csv","r") as file:
		reader = csv.DictReader(file, delimiter=';')
		route_cover_metrics = []
		for r in reader:
			route_cover_metrics.append(r)

	metrics = []

	# Расчитаываем метрики для маршрутов
	for route in routes:
		#print(route)
		route_id = route['properties']['ID']

		# Интеравал
		avg_interval = default_interval
		# Число рейсов
		tracks_cnt = 18*60/avg_interval-1 # 18 часов * 60 минут / средний интервал - 1 (первый рейс)

		# Считаем средний интервал движения
		metric = {"metric_code":"avg_interval","route_id":route_id,"metric_value":avg_interval}
		metrics.append(metric)
		
		# Считаем длину маршрута
		route_length = round(turf.get_line_length(shape(route['geometry'])),2)
		metric = {"metric_code":"route_length","route_id":route_id,"metric_value":route_length}
		metrics.append(metric)
		
		# Считаем стоимость маршрута
		route_cost = round(tracks_cnt*route_length*110)
		metric = {"metric_code":"route_cost","route_id":route_id,"metric_value":route_cost}
		metrics.append(metric)

		# Считаем прямолинейность
		alter_route = next((x for x in alter_routes if x['properties']['route_id'] == route_id),None)
		if alter_route != None:
			straightness = round(alter_route['properties']['distance']/route_length,2)
			metric = {"metric_code":"straightness","route_id":route_id,"metric_value":straightness}
			metrics.append(metric)
		
		# Число жителей на рейс
		mp = next((x for x in route_cover_metrics if x['isochrone_code'].split('-')[1] == route_id and x['metric_code'] == 'houses_population'),None)
		if mp != None:
			population = mp['metric_value']
			population_per_track = round(float(population)/tracks_cnt,2)
			metric = {"metric_code":"population_per_track","route_id":route_id,"metric_value":population_per_track}
			metrics.append(metric)

		# Число рейсов в час
		metric = {"metric_code":"tracks_per_hour","route_id":route_id,"metric_value":round(60/avg_interval)}
		metrics.append(metric)

	# Записываем результат в файл
	with open("../out/metrics/route_metrics.csv","w") as file:
		fieldnames = metrics[0].keys()
		writer = csv.DictWriter(file, fieldnames=fieldnames,delimiter=';')
		writer.writeheader()
		for m in metrics:
			writer.writerow(m)

	print("Finish generate_route_metrics")


# ============ Метрики городов ==================#
# Площадь покрытия
# Отношение площади покрытия остновок к площади города
# Дома в зоне покрытия
# Население в зоне покрытия
# Доля населения в зоне покрытия
# Число остановок на 100тыс жителей
# Число маршрутов на 100тыс жителей

def generate_city_metrics():
	# Извелкаем параметры
	with open("../in/params/params.json") as file:
		params = json.load(file)
		city_code = params['city_code']
		city_area = params['area']

	# Дома города
	with open("../out/houses/houses.geojson") as file:
		houses = json.load(file)['features']

	# Считаем начелением по домам
	population = reduce(lambda x,y: x+y, map(lambda x: x['properties']['population'],houses),0)
	
	# Изохрон покрытия остановками
	with open("../out/stops_cover/stops_cover.geojson") as file:
		stops_cover = shape(json.load(file)['features'][0]['geometry'])
	
	metrics = []

	# Площадь покрытия
	cover_area = round(turf.calculateArea(stops_cover)/1000000,2)
	metric = {"metric_code":"cover_area","city_code":city_code,"metric_value":cover_area}
	metrics.append(metric)

	# Отношение площади покрытия остновок к площади города
	cover_area_share = round(cover_area/city_area*100,2)
	metric = {"metric_code":"cover_area_share","city_code":city_code,"metric_value":cover_area_share}
	metrics.append(metric)
	
	# Дома в зоне покрытия
	cover_info = get_objects_inside_info(houses,stops_cover)
	metric = {"metric_code":"cover_houses","city_code":city_code,"metric_value":cover_info['cnt']}
	metrics.append(metric)
	
	# Доля домов в зоне покрытия
	cover_houses_share = round(cover_info['cnt']/len(houses)*100,2)
	metric = {"metric_code":"cover_houses_share","city_code":city_code,"metric_value":cover_houses_share}
	metrics.append(metric)

	# Население в зоне покрытия
	metric = {"metric_code":"cover_population","city_code":city_code,"metric_value":cover_info['population']}
	metrics.append(metric)

	# Доля населения в зоне покрытия
	cover_population_share = round(cover_info['population']/population*100,2)
	metric = {"metric_code":"cover_population_share","city_code":city_code,"metric_value":cover_population_share}
	metrics.append(metric)

	# Загружаем список остановок
	with open("../out/stations/geojson/stations.geojson") as file:
		stations = json.load(file)['features']

	# Число остановок на 100тыс жителей
	stations_per_100k = round(len(stations)/(population/100000),2)
	metric = {"metric_code":"stations_per_100k","city_code":city_code,"metric_value":stations_per_100k}
	metrics.append(metric)

	# Загружаем список маршрутов
	with open("../out/routes/geojson/routes.geojson") as file:
		routes = json.load(file)['features']

	# Число маршрутов на 100тыс жителей
	routes_per_100k = round(len(routes)/(population/100000),2)
	metric = {"metric_code":"routes_per_100k","city_code":city_code,"metric_value":routes_per_100k}
	metrics.append(metric)

	# Записываем результат в файл
	with open("../out/metrics/city_metrics.csv","w") as file:
		fieldnames = metrics[0].keys()
		writer = csv.DictWriter(file, fieldnames=fieldnames,delimiter=';')
		writer.writeheader()
		for m in metrics:
			writer.writerow(m)

	print("Finish generate_city_metrics")

# =========================== 
if __name__ == '__main__':

	print("Start Metrics")
	#run_isochrone_metrics_in_threads("walking",6)
	
	#generate_stations_reachable()
	#generate_public_coverage()
	#generate_population_to_tracks()
	#run_isochrone_metrics_in_threads("route_cover",6)
	#generate_station_metrics(default_interval=10)
	#generate_route_metrics(default_interval=10)
	#generate_city_metrics()

	#with open("../out/metrics/metrics_public_transport.csv","r") as file:
	#	reader = csv.DictReader(file, delimiter=';')
	#	for row in reader:
	#		print(row)

	#run_isochrone_metrics_in_threads("walking",1)

	# with open("../out/isochrones/isochrones_driving.csv","r") as file:
	# 	reader = csv.DictReader(file, delimiter=';')
	# 	isochrones = []
	# 	for row in reader:
	# 		isochrones.append(row) 
	# 	#print(len(isochrones))
	# 	start = datetime.now()
	# 	generate_isohrone_metrics(isochrones[:100], 'driving', 1)
	# 	finish = datetime.now()
	# 	print("Done in ",finish-start)