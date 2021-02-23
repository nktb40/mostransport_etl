import pandas
import json
import mapbox
import time 
from datetime import datetime
import numpy as np
import multiprocessing 
import glob
import geopandas
from shapely.geometry import *
import os
import math
from shapely.ops import unary_union
import shapely
from csv import DictReader, DictWriter
import copy

city_code = ""
threads_num = 6
default_interval = 10

# Загрузка параметров из файла с параметрами
def init():
	global city_code, threads_num, default_interval

	with open("../in/params/params.json", 'r') as file:
		city_params = json.load(file)
		keys = city_params.keys()
		city_code = city_params['city_code']

		if 'default_interval' in keys:
			default_interval = city_params['default_interval']
	
	with open("../global_params.json", 'r') as file:
		global_params = json.load(file)
		threads_num = global_params['threads_num']

# Функция генерации изохронов маршрутов
def generate_route_isochrones():
	routes = read_routes()
	route2stops = read_route2stops()
	walk_iso = read_walk_iso()

	route_cover = pandas.DataFrame(columns=['city_code','id','station_id','contour','profile','geometry'])

	# Находим пешие изохроны для остановок и объединяем их в изохрон маршрута
	for i_r, route in routes.iterrows(): 

		route_id = route['id']
		#print(route_id)
		ids = route2stops.query('route_id == @route_id')['station_id'].tolist()
		
		if len(ids) > 0:
			iso = walk_iso.query('station_id in @ids and geometry != None')['geometry'].tolist()
			union = unary_union(iso)

			route_iso = {
				'profile': 'route_cover',
				'contour': 5,
				'route_id': str(route_id),
				'id': city_code+"-"+str(route_id)+'-route_cover-5',
				'geometry': json.dumps(shapely.geometry.mapping(union), separators=(',',':'))
			}

			route_cover = route_cover.append(route_iso, ignore_index=True)
	
	route_cover['city_code'] = city_code
	route_cover.to_csv("../out/isochrones/isochrones_route_cover.csv", sep=';')


# Герерация изохронов: Пеших, Вело и Авто
def generate_isochrones(city_code,profile, points, mode, times):
	# mode: w - write, a - append
	#times = '10,20,30'
	points['id'] = points['id'].astype(str)
	points['longitude'] = points['longitude'].astype(str)
	points['latitude'] = points['latitude'].astype(str)
	
	row_num = 0
	points_len = str(len(points))
	print("Start generating "+profile+" isochrones: "+points_len)
	file  = open('../out/isochrones/isochrones_'+profile+'.csv', mode)
	
	if mode == "w":
		file.write('city_code;id;contour;profile;station_id;geometry\n')

	for index, row in points.iterrows():
		iso = mapbox.get_iso(profile, row['longitude'],row['latitude'],times)
		features = iso["features"]
		
		for i, f in enumerate(features):
			contour = str(f["properties"]["contour"])
			station_id = row["id"]
			id = city_code+"-"+station_id+"-"+profile+"-"+contour
			geometry = json.dumps(f["geometry"], separators=(',',':'))

			str_out = city_code+";"+id+";"+contour+";"+profile+";"+station_id+";"+geometry+"\n"

			file.write(str_out)

		row_num += 1
		print("Iso. "+profile+". Generated: "+str(row_num)+" points from "+points_len+" ("+str((row_num)*3)+" isochrones). Last point id: "+str(row["id"]))
		time.sleep(0.2)

	file.close()


def deduplicate(list):
	new_list = []
	for item in list:
		if item not in  new_list:
			new_list.append(item)
	return new_list

# Функция загрузки списка остановок
def read_stations():
	stations = pandas.read_csv("../out/stations/csv/stations.csv", sep=";")
	stations['geometry'] = stations['geometry'].apply(lambda x: shape(json.loads(x)))
	stations['id'] = stations['id'].astype(str)
	stations['route_numbers'] = stations['route_numbers'].apply(lambda x: json.loads(x))
	return stations

# Функция загрузки списка маршрутов
def read_routes():
	routes = pandas.read_csv("../out/routes/csv/routes.csv", sep=";")
	routes['geometry'] = routes['geometry'].apply(lambda x: shape(json.loads(x)))
	return routes

# Функция загрузки связей маршрутов и остановок
def read_route2stops():
	route2stops = pandas.read_csv('../out/route2stops/route2stops.csv', sep=";")
	route2stops['station_id'] = route2stops['station_id'].astype(str)
	#route2stops['station_ids'] = route2stops['station_ids'].apply(json.loads)
	return route2stops

# Функция загрузки 5-ти минутных пеших изохронов
def read_walk_iso():
	walk_iso = pandas.read_csv("../out/isochrones/isochrones_walking.csv", sep=";").query('contour == 5')
	walk_iso['geometry'] = walk_iso['geometry'].apply(lambda x: shape(json.loads(x)))
	return walk_iso

# Функция для нахождения соседних остановок в радиусе пешего 5-ти минутного изохрона
def generate_station_neighbors(stations):

	print("Start function generate_station_neighbors: ", datetime.now())

	#stations = read_stations() #.query('id == "6052394926421880"')
	stations = geopandas.GeoDataFrame(stations)

	# Пешие изохроны 5-ти минутные
	walk_iso = read_walk_iso()

	station_neighbors = pandas.DataFrame(columns=['station_id','neighbor_ids'])
	st_len = len(stations)

	for i_s, station in stations.iterrows():
		print(str(i_s)+" from "+str(st_len)+" last: "+str(station['id']))
		station_id = station['id']
		iso = walk_iso.query('station_id == @station_id')
		
		if len(iso) > 0:
			iso = iso.iloc[0]
			geometry = iso['geometry']
			if not(geometry == None or geometry.is_empty == True):
				bounds = geometry.bounds
				in_bounds = stations.cx[bounds[0]:bounds[2], bounds[1]:bounds[3]]

				if len(in_bounds) > 0:
					in_bounds['in_polygon_flag'] = in_bounds.apply(lambda p: p['geometry'].within(geometry), axis=1)
					in_polygon = in_bounds.query('in_polygon_flag == True and id != @station_id')

					neighbors = in_polygon['id'].tolist()

					lnk_station = {
						'station_id': station_id,
						'neighbor_ids': neighbors
					}

					station_neighbors = station_neighbors.append(lnk_station, ignore_index=True)
	
	station_neighbors['neighbor_ids'] = station_neighbors['neighbor_ids'].apply(lambda x: json.dumps(x))
	station_neighbors.to_csv('../out/station_neighbors/station_neighbors.csv',sep=";", columns=['station_id','neighbor_ids'])
	print("Finish function generate_station_neighbors: ", datetime.now())


# Генерация изохронов для ОТ
def generate_isochrones_public_transport(stops_in, part, times, with_interval,with_changes):
	# stops_in DataFrame - спсиок остановок, для которых нужно рассчитать изохроны
	# part  String - номер части общего списка остановок (при параллельном расчёте)
	# times '10,20,30' - Время изохронов, для которых нужно сделать расчёт
	# with_interval "1" - да / "0" - нет - учитывать или нет интервалы
	# with_changes "1" - да / "0" - нет - учитывать или нет пересадки

	stops = read_stations()
	routes = read_routes()
	route2stops = read_route2stops()
	walk_iso = read_walk_iso()

	if with_changes == "1":
		neighbors = pandas.read_csv('../out/station_neighbors/station_neighbors.csv', sep=";")
	else: 
		neighbors = []

	if with_interval == "1" and os.path.exists('../in/intervals/intervals.csv') == True:
		intervals = pandas.read_csv('../in/intervals/intervals.csv', sep=";")
	else:
		intervals = []

	public_isochrones =[]

	file_out = open("../out/isochrones/public_transport/tmp/isochrones_public_transport_int"+with_interval+"_wch"+with_changes+"_p"+part+".csv", 'w')
	file_out.write('city_code;id;contour;profile;station_id;with_interval;with_changes;geometry;properties\n')

	ind = 0
	stop_in_cnt = len(stops_in)

	for i_s, stop in stops_in.iterrows():
		ind +=1
		print("Iso. Public Transport. Proc: "+part+". Stop: "+str(ind)+" from: "+str(stop_in_cnt)+". Last stop ID: "+str(stop['id']))
		
		# Массив остановок изохронов
		connected_stops = get_stops_on_route_inside_isochrone(stop, route2stops, [], intervals, with_interval, times, [],0)

		# Добавляем связанные остановки с учётом пересадок
		if with_changes == "1":
			cp = copy.deepcopy(connected_stops)
			for c in cp: 
				cur_routes = stop['route_numbers']
				items = c['items']
				for i, item in enumerate(items):
					id = item['id']
					distance = item['distance']

					# Находим связанные остановки для пересадочных маршрутов без смены остановки
					con_stop = stops.query('id == @id').iloc[0]
					connected_stops = get_stops_on_route_inside_isochrone(con_stop, route2stops, cur_routes, intervals, with_interval, c['contour'], connected_stops, distance)
					route_nums = con_stop['route_numbers']
					cur_routes = list(set(route_nums + cur_routes))

					# Находим связанные остановки для пересадочних маршрутов со сменой остановки (соседние остановки)
					filtered_neibs = neighbors.query('station_id == @id')['neighbor_ids']
					if len(filtered_neibs) > 0:
						filtered_neibs = json.loads(filtered_neibs.iloc[0])
						for n_id in filtered_neibs:
							neib_stop = stops.query('id == @n_id').iloc[0]
							connected_stops = get_stops_on_route_inside_isochrone(neib_stop, route2stops, cur_routes, intervals, with_interval, c['contour'], connected_stops, distance+5)
							
							route_nums = neib_stop['route_numbers']
							cur_routes = list(set(route_nums + cur_routes))
		
		# Находим пешие изохроны для остановок, чтобы построить из них автобусные и записываем в файл
		for c in connected_stops: 
			#print(c)
			ids = c['ids']
			route_codes = c['routes']
			properties = {'stop_ids': ids, 'routes': route_codes}
			iso = walk_iso.query('station_id in @ids and geometry != None')['geometry'].tolist()
			#iso = list(map(lambda x: asPolygon(np.array(json.loads(x))), iso))
			union = unary_union(iso)

			id = city_code+'-'+str(stop['id'])+'-public_transport-'+str(c['contour']+'-'+with_interval+'-'+with_changes)
			geometry = json.dumps(shapely.geometry.mapping(union), separators=(',',':'))
			str_out = city_code+";"+id+";"+str(c['contour'])+";"+"public_transport"+";"+str(stop['id'])+";"+with_interval+";"+with_changes+";"+geometry+";"+json.dumps(properties,ensure_ascii=False)+"\n"

			file_out.write(str_out)

	file_out.close()

# Находим остановки на пути выбранных маршрутов внутри изохрона
def get_stops_on_route_inside_isochrone(station, route2stops, cur_routes, intervals, with_interval, times, connected_stops, dist_from_start_point):

	# Определяем маршруты и направления, в которых учиствует выбранная остановка
	station_id = station['id']
	routes = route2stops.query('station_id == @station_id and route_code not in @cur_routes')

	# Запускаем цикл по каждому маршруту для определения остановок внутри ихохрона
	for i_r, route in routes.iterrows():
		
		route_id = route['route_id']
		route_code = route['route_code']
		seq_no = route['seq_no']
		track_no = route['track_no']

		# Определяем параметры верменных контуров изохронов
		if with_interval == "1":
			if len(intervals) > 0:
				interval_list = intervals.query('route_code == @route_code')['avg_fact_interval'].tolist()
				if len(interval_list) == 0:
					interval = default_interval/2
				else:
					interval = round(int(interval_list[0])/2)
			else:
				interval = default_interval/2
		else:
			interval = 0
			
		times_array = list(map(lambda t: {"time":t, "t_modif": int(t) - interval - dist_from_start_point }, times.split(',')))

		# Параметры для цикла
		route_time = 0
		breaks = []

		# Находим все остановки на направлении маршрута дальше выбранной остановки
		for i_s, r2s in route2stops.query('route_id == @route_id and track_no == @track_no and seq_no >= @seq_no').iterrows():
			
			route_time += r2s['route_time']
			stop_id = r2s['station_id']
			item = {'id':stop_id, 'distance':route_time}

			for t in times_array:

				if route_time <= t['t_modif']:
					contour = [c for c in connected_stops if c['contour'] == t['time']]

					# Добавляем ID остановок каждого контура в свой массив
					if contour == []:
						connected_stops.append({'contour':t['time'], 'ids':[stop_id], 'items':[item], 'routes':[route_code]})
					else:
						contour = contour[0]

						contour['ids'] = list(set(contour['ids'] + [stop_id]))
						contour['routes'] = list(set(contour['routes'] + [route_code]))
						if dist_from_start_point == 0:
							contour['items'] = deduplicate(contour['items'] + [item])
				else:
					breaks.append(t['time'])
					breaks = list(set(breaks))

			if len(breaks) == len(times_array):
				break
	return connected_stops

# функция запуска расчёта изохронов
def run_public_transport(stations):
	# Step 2.4: Загружаем изохроны Public transport

	# Step 2.4.1: Изохроны без интервалов и пересадок
	run_public_transport_in_threads(stations, threads_num,"10,20,30", "0", "0")

	# Step 2.4.2: Находим соседние остановки для всех остановок
	generate_station_neighbors(stations)

	# Step 2.4.3: Изохроны без интервалов, но с пересадками
	run_public_transport_in_threads(stations, threads_num, "10,20,30", "0", "1")

	# Step 2.4.4: Изохроны с интервалами, но без пересадок
	if (os.path.exists('../in/intervals/intervals.csv')) or (math.isnan(default_interval) == False and default_interval != 0):
		run_public_transport_in_threads(stations, threads_num, "10,20,30", "1", "0")

	# Step 2.4.5: Изохроны с интервалами и с пересадками
	if (os.path.exists('../in/intervals/intervals.csv')) or (math.isnan(default_interval) == False and default_interval != 0):
		run_public_transport_in_threads(stations, threads_num, "10,20,30", "1", "1")


# Запуск расчёта изохронов в многопоточном режиме
def run_public_transport_in_threads(df, threads_num, times, with_interval, with_changes):
	print("Start gen iso public_transport. With interval: "+with_interval+" with_changes: "+with_changes+". Time:",datetime.now())
	df_split = np.array_split(df, threads_num)
	processes = []
	for i in range(0,threads_num):
	    print("Iso Public transport. Part: "+str(i+1)+" len: "+str(len(df_split[i])))
	    p = multiprocessing.Process(target=generate_isochrones_public_transport, args=(df_split[i], str(i+1), times, with_interval, with_changes))
	    processes.append(p)
	    p.start()
	
	for process in processes:
		process.join()

	out_files = glob.glob("../out/isochrones/public_transport/tmp/*.csv")
	out_df = pandas.DataFrame()
	for file_name in out_files:
	 	out_df = out_df.append(pandas.read_csv(file_name, sep=";"), sort=False)
	 	os.remove(file_name)

	out_df.to_csv("../out/isochrones/public_transport/isochrones_public_transport_int"+with_interval+"_wch"+with_changes+".csv", sep=";")

	print("Finish gen iso public_transport. With interval: "+with_interval+" with_changes: "+with_changes+". Time:",datetime.now())


# Расчёт данных для зоны покрытия остановок (изохрон покрытия)
def get_stops_cover_iso():
	print("Start get_stops_area_iso")

	iso_list = []

	with open('../out/isochrones/isochrones_walking.csv', 'r') as read_obj:
		reader = DictReader(read_obj, delimiter=';')
		for row in reader:
			if row['contour'] == '5':
				geometry = shape(json.loads(row['geometry']))
				iso_list.append(geometry)
	
	iso_union = unary_union(iso_list)
	iso_json = json.dumps(geopandas.GeoSeries([iso_union]).__geo_interface__, separators=(',',':'))

	os.system('mkdir ../out/stops_cover')
	file_out = open('../out/stops_cover/stops_cover.geojson', 'w')
	file_out.write(iso_json)
	file_out.close()

	print("Finish get_stops_area_iso")
