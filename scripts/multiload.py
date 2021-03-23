# Модуль для выполнения расчёта по нескольким городам в цикле
import os
import json
import index as singleload
import reforma
import scraper
import mapbox
import postgres
import isochrones
import dtp
import metrics
from functools import reduce
import math

# Получаем список городов из параметров
city_codes = []
arch_dir = ""

# Функция чтения параметров
def init():
	global city_codes, arch_dir

	with open("../global_params.json", 'r') as file:
		params = json.load(file)
		city_codes = params['city_codes']
		
		arch_dir = params['archive_folder_dir']
		print(arch_dir)

# Добавление новых параметров в params.json
def update_population_params():
	with open("../in/params/params.json", 'r') as file:
		city_params = json.load(file)

	with open("../population.json", 'r') as file:
		pop_params = next(x for x in json.load(file) if x['city_code'] == city_params['city_code'])

	city_params['area'] = pop_params['area']
	city_params['population'] = pop_params['population']

	with open('../in/params/params.json', 'w') as file_out:
		file_out.write(json.dumps(city_params,ensure_ascii=False))

def update_houses_population():
	with open("../in/params/params.json", 'r') as file:
		city_params = json.load(file)

	with open("../out/houses/houses.geojson", 'r') as file:
		src = json.load(file)

	# Расчитываем жилую площадь на одного жителя
	area_residential = reduce((lambda x, y: x+y), map(lambda x: float(x['properties']['area_residential'].replace(',','.')) if x['properties']['area_residential'] != '' else 0, src['features']))
	area_per_pers = round(area_residential/city_params['population'],2)

	# Расчитываем кол-во жителей для каждого дома
	for house in src['features']:
		house['properties']['population'] = math.ceil(float(house['properties']['area_residential'].replace(',', '.'))/area_per_pers) if house['properties']['area_residential'] != '' else 0

	with open('../out/houses/houses.geojson', 'w') as file_out:
		file_out.write(json.dumps(src,ensure_ascii=False))


if __name__ == '__main__':
	# Инициализируем параметры
	init()
	print(city_codes)

	# Выполняем список функций по каждому городу
	for city_code in city_codes:
		print("City code: ",city_code)
		#1. Удаляем папки с входными и выходными данными
		os.system("rm -rf ../in") 
		os.system("rm -rf ../out")

		# 2. Копируем архивы с входными и выходными данными по городу
		os.system("cp "+arch_dir+"/"+city_code+"/* ../")

		# 3. Распаковываем папки IN и OUT
		os.system("unzip ../"+city_code+"_IN -d ../")	
		os.system("unzip ../"+city_code+"_OUT -d ../")

		# 4. Удаляем архивы
		os.system("rm ../*.zip")

		# 5. Запускаем расчёт функций
		singleload.init()
		city_name, region_name = singleload.get_params()

		# Загрузк маршрутов, Остановок и их связей
		#singleload.load_route_and_stations()
		#singleload.get_routes_attributes()
		#postgres.upload_stations(city_code)
		#postgres.upload_routes(city_code)
		#postgres.upload_lnk_station_routes(city_code)
		#singleload.generate_alternative_routes()
		#singleload.generate_routes_geojson()
		#singleload.generate_stations_geojson()

		# Загрузка изохронов
		# isochrones.init()
		# isochrones.generate_route_isochrones()
		# metrics.run_isochrone_metrics_in_threads("route_cover",6)
		# postgres.upload_isochrones(city_code, "route_cover")
		# postgres.upload_metrics("route_cover")

		# # Загружаем дома
		# reforma.get_city_houses(city_code,city_name,region_name)
		# # Загружаем дома далеко от остановок
		# isochrones.get_stops_cover_iso()
		# reforma.get_houses_far_from_stops()
		# mapbox.create_houses_far_stops_tileset(city_code)
		# postgres.upload_houses(city_code)

		# # Загрузка слоя с ДТП
		# # scraper.download_dtp_file(region_name)
		# # #os.system("mv ../out/dtp_map/dtp_map.geojson ../out/dtp_map/dtp_map_src.geojson")
		# # dtp.transform_dtp()
		# # mapbox.create_dtp_map_tileset(city_code)

		# # Загрузка очагов ДТП
		# # scraper.download_dtp_ochagi(city_code,region_name)
		# # mapbox.run_create_tileset(city_code,"dtp_ochagi")

		# # # Загрузка камер
		# # scraper.download_traffic_cameras(city_code,region_name)
		# # mapbox.run_create_tileset(city_code,"traffic_cameras")

		# # Обновление парметров города
		# update_population_params()
		# postgres.upload_city()

		# # Обновление параметров домов
		# update_houses_population()
		# postgres.upload_houses(city_code)

		# # Step 7.1: Загрузка метрик для изохронов
		# metrics.run_isochrone_metrics_in_threads("walking",6)
		# metrics.run_isochrone_metrics_in_threads("cycling",6)
		# metrics.run_isochrone_metrics_in_threads("driving",6)
		# metrics.run_isochrone_metrics_in_threads("public_transport",6)
		# metrics.run_isochrone_metrics_in_threads("route_cover",6)

		# # Загрузка метрик для остановок
		# metrics.generate_station_metrics(default_interval=10)

		# # Загрузка метрик для маршрутов
		# metrics.generate_route_metrics(default_interval=10)

		# # Загрузка метрик для города 
		# metrics.generate_city_metrics()

		# # Загрузка метрик в БД
		# postgres.upload_metrics("walking")
		# postgres.upload_metrics("cycling")
		# postgres.upload_metrics("driving")
		# postgres.upload_metrics("public_transport")
		# postgres.upload_metrics("route_cover")	
		# postgres.upload_station_metrics()
		# postgres.upload_route_metrics()
		# postgres.upload_city_metrics()

		# # 6. Добавляем папки IN и OUT в zip архив
		# os.system("cd ..; zip -r "+city_code+"_IN in")
		# os.system("cd ..; zip -r "+city_code+"_OUT out")

		# # 7. Перемещаем zip файлы в архив
		# os.system("cd "+arch_dir+"; mkdir "+city_code)
		# os.system("mv ../"+city_code+"_IN.zip "+arch_dir+"/"+city_code+"/")
		# os.system("mv ../"+city_code+"_OUT.zip "+arch_dir+"/"+city_code+"/")