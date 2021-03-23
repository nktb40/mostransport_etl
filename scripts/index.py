#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas
import os
import json
#import copy
from datetime import datetime
import math
import shapely
import numpy as np
from shapely.geometry import *
#from shapely.ops import unary_union
import geopandas
import multiprocessing 
import glob
from csv import DictReader, DictWriter
import mapbox
import turf
import reforma
import postgres
import isochrones
import scraper
import metrics
import dtp


# Параметры
city_code = ""
city_name = ""
region_name = ""
threads_num = 6
default_interval = 10
arch_dir = ""

# Загрузка параметров из файла с параметрами
def init():
	global city_code, city_name, region_name, threads_num, default_interval, arch_dir

	with open("../in/params/params.json", 'r') as file:
		city_params = json.load(file)
		keys = city_params.keys()

		city_code = city_params['city_code']
		city_name = city_params['city_name']
		region_name = city_params['region_name']

		if 'default_interval' in keys:
			default_interval = city_params['default_interval']
	
	with open("../global_params.json", 'r') as file:
		global_params = json.load(file)
		threads_num = global_params['threads_num']
		arch_dir = global_params['archive_folder_dir']

	print("Param city_code =",city_code)
	print("Param default_interval =",default_interval)


def get_params():
	return city_name, region_name

# Функция загрузки Маршрутов, Остановок и их связей
def load_route_and_stations():
	print("Start function load_route_and_stations: ", datetime.now())

	src_files = glob.glob("../in/routes_and_stops/*.json")

	routes = geopandas.GeoDataFrame(columns=['city_code','id','route_number','type_of_transport','route_name','route_code','circular_flag','geometry'])
	stations = geopandas.GeoDataFrame(columns=['city_code','id','station_name','route_numbers','geometry'])
	route2stops = pandas.DataFrame(columns=['city_code','route_id','station_id','route_code','route_type','track_no','seq_no'])

	for file_name in src_files:
		file = json.load(open(file_name, 'r'))
		print(file_name)
		#print(file['result']['items'][0]['directions'][0]['platforms'][0].keys())
		#print(file['result']['items'][0]['directions'][0]['platforms'][1])

		item = file['result']['items'][0]

		# Загружаем данные по маршруту

		# Приводим кольцевые маршруты к одному формату
		for d in item['directions']:
			if d['type'] == 'loop':
				d['type'] = 'circular'

		# фильтруем направления, чтобы исключить дополнительные маршруты
		item['directions'] = list(map(lambda x: x[1], list(filter(lambda x: x[1]['type'] != "additional" and not (x[1]['type'] == 'circular' and x[0] > 0), enumerate(item['directions'])))))
		
		# Сортируем направления, чтобы сначала было прямое, затем обратное
		item['directions'].sort(key=lambda x: 0 if x['type'] == 'circular' else 1 if x['type'] == 'forward' else 2, reverse=False)
		# Выгружаем геометрию маршрута
		geometry = list(map(lambda x: shapely.wkt.loads(x['geometry']['selection']),item['directions']))
		geometry = MultiLineString(geometry)

		# Тип транспорта маршрута
		if item['subtype'] == 'bus':
			route_code = 'А'+str(item['name'])
			type_of_transport = 'автобус'
		elif item['subtype'] == 'trolleybus':
			route_code = 'Тб'+str(item['name'])
			type_of_transport = 'троллейбус'
		elif item['subtype'] == 'tram':
			route_code = 'Тм'+str(item['name'])
			type_of_transport = 'трамвай'
		elif item['subtype'] == 'shuttle_bus':
			route_code = 'Ш'+str(item['name'])
			type_of_transport = 'шатл'
		elif item['subtype'] ==  'premetro':
			route_code = 'Мт'+str(item['name'])
			type_of_transport = 'метротрам'
		elif item['subtype'] ==  'metro':
			route_code = 'М'+str(item['name'])
			type_of_transport = 'метро'
		else:
			print("New route type: ",item['subtype'],item['name'])

		# Флаг кольцевого маршрута
		if item['directions'][0]['type'] == 'circular':
			circular_flag = 1
		else:
			circular_flag = 0

		route_id = item['id'].split("_")[0]

		route = {
			'id': route_id,
			'route_number': item['name'], 
			'type_of_transport': type_of_transport,
			'route_name': item['from_name'] + " - "+item['to_name'],
			'route_code': route_code,
			'circular_flag':circular_flag,
			'bbox': get_bbox(geometry),
			'geometry': geometry
			#'geometry': json.dumps(shapely.geometry.mapping(geometry), separators=(',',':'))
		}

		#print(route)
		routes = routes.append(route, ignore_index=True)

		# загружаем данные по остановкам
		for i_d, direction in enumerate(item['directions']):
			# Определяем код направления маршрута
			direction_type =  direction['type']

			if direction_type == 'circular':
				track_no = 1
			elif direction_type == 'forward':
				track_no = 1
			elif direction_type == 'backward':
				track_no = 2

			# Определяем остановки
			for i_p, platform in enumerate(direction['platforms']):

				station_id = platform['id']
				station = stations.query('id == @station_id')

				if station.empty:
					geometry = shapely.wkt.loads(platform['geometry']['selection'])
					station = {
						'id': station_id,
						'station_name': platform['name'],
						'route_numbers': [route['route_code']],
						'longitude': geometry.coords[0][0],
						'latitude': geometry.coords[0][1],
						#'geometry': json.dumps(shapely.geometry.mapping(geometry), separators=(',',':'))
						'geometry': geometry
					}

					stations = stations.append(station, ignore_index=True)
				else:
					station = station.iloc[0]
					if route['route_code'] not in station['route_numbers']:
						station['route_numbers'].append(route['route_code'])

				# Загружаем связь между маршрутом и остановкой
				r2s = route2stops.query('route_id == @route_id and track_no == @track_no and station_id == @station_id')
				
				if r2s.empty:
					r2s = {
							'route_id': route_id,
							'station_id': station_id,
							'route_code': route_code,
							'route_type': direction_type,
							'track_no': track_no,
							'seq_no': i_p
					}
					route2stops = route2stops.append(r2s, ignore_index=True)
	
	# Выгружаем в файл связь маршрутов и остановок
	route2stops['city_code'] = city_code
	r2s_with_time = pandas.DataFrame(columns=['city_code','route_id','station_id','route_code','route_type','track_no','seq_no','route_time'])

	for i, r2s in route2stops.iterrows():
		if i == 0:
			r2s['route_time'] = None
			r2s['distance'] = None
		else:
			prev_stop = route2stops.iloc[i-1]
			if r2s['route_id'] == prev_stop['route_id'] and r2s['track_no'] == prev_stop['track_no']:
				route_id = r2s['route_id']
				route = routes.query('id == @route_id').iloc[0]
				line = route['geometry'][r2s['track_no']-1]

				stop_id = r2s['station_id']
				prev_stop_id = prev_stop['station_id']

				startPt = stations.query('id == @prev_stop_id').iloc[0]
				stopPt = stations.query('id == @stop_id').iloc[0]

				new_line = turf.line_slice(startPt, stopPt, line)
				new_line_len = turf.get_line_length(new_line)
				route_time = round(new_line_len/15*60)

				r2s['route_time'] = route_time
				r2s['distance'] = round(new_line_len*1000)
			else:
				r2s['route_time'] = None
				r2s['distance'] = None
		r2s_with_time = r2s_with_time.append(r2s[['city_code','route_id','station_id','route_code','route_type','track_no','seq_no','route_time','distance']])

	r2s_with_time.to_csv("../out/route2stops/route2stops.csv", sep=";")

	# Выгружаем в файл маршруты
	routes['city_code'] = city_code
	routes['geometry'] = routes['geometry'].apply(lambda x: json.dumps(shapely.geometry.mapping(x), separators=(',',':')))
	routes.to_csv("../out/routes/csv/routes.csv", sep=";")
	
	# Выгружаем в файл остановки
	stations['city_code'] = city_code
	stations['route_numbers'] = stations['route_numbers'].apply(lambda x: json.dumps(x,ensure_ascii=False))
	stations['geometry'] = stations['geometry'].apply(lambda x: json.dumps(shapely.geometry.mapping(x), separators=(',',':')))
	stations.to_csv("../out/stations/csv/stations.csv", sep=";")
	
	print("Finish function load_route_and_stations: ",datetime.now())

def get_bbox(geometry):
	b = shape(geometry).bounds 
	return [[b[0],b[1]],[b[2],b[3]]]

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

# Функция генерации geojson файлов для остановок
def generate_stations_geojson():
	print("Start function generate_stations_geojson: ",datetime.now())

	df_in = read_stations()[['id','city_code','station_name','route_numbers','geometry']]
	df_in['id'] = df_in['id'].astype(str)
	df_in['route_numbers'] = df_in['route_numbers'].apply(lambda x: "; ".join(x))
	df_in.rename(columns={"id": "global_id", "station_name":"StationName", "route_numbers":"RouteNumbers"}, errors="raise",inplace=True)
	
	df_in["tippecanoe"] = json.dumps({"layer" : "bus_stops"}, separators=(',',':'))
	df_out = geopandas.GeoDataFrame(df_in)
	df_out.to_file("../out/stations/geojson/stations.geojson", driver='GeoJSON')

	print("Finish function generate_stations_geojson: ",datetime.now())

# Функция генерации geojson файлов для маршрутов
def generate_routes_geojson():
	print("Start function generate_routes_geojson: ",datetime.now())

	df_in = read_routes()[['id','city_code','route_name','route_code','geometry']]
	df_in['id'] = df_in['id'].astype(str)
	df_in.rename(columns={"id": "ID", "route_name":"RouteName"}, errors="raise",inplace=True)

	df_in["tippecanoe"] = json.dumps({"layer" : "routes"}, separators=(',',':'))
	df_out = geopandas.GeoDataFrame(df_in)
	df_out.to_file("../out/routes/geojson/routes.geojson", driver='GeoJSON')

	print("Finish function generate_routes_geojson: ",datetime.now())

# Генерация маршрутов по автомобильным доргам для расчётв прямолинейности
def generate_alternative_routes():
	print("Start function generate_alternative_routes:",datetime.now())
	routes = read_routes()
	routes.query('circular_flag == False', inplace=True)
	
	auto_routes = geopandas.GeoDataFrame(columns=['route_id','route_code','distance','geometry'])

	routes_len = len(routes)
	for i_r, route in routes.iterrows():
		print(str(i_r)+" from "+str(routes_len)+ " last: "+route['route_code'])
		
		directions = route.geometry

		if len(directions) == 1:
			route_line = directions[0]
		else:
			direct_len = directions[0].length
			back_len = directions[1].length

			if direct_len > back_len:
				route_line = directions[0]
			else:
				route_line = directions[1]

		first_point = str(route_line.coords[0][0])+","+str(route_line.coords[0][1])
		last_point = str(route_line.coords[-1][0])+","+str(route_line.coords[-1][1])

		result = mapbox.get_route(first_point,last_point)
		route_obj = sorted(result['routes'], key=lambda r: r['distance'])[0]
		
		auto_route = {
			'route_id': route['id'],
			'route_code': route['route_code'],
			'distance':round(route_obj['distance']/1000,2),
			'geometry':shape(route_obj['geometry'])
		}

		auto_routes = auto_routes.append(auto_route, ignore_index=True)
	
	#print(auto_routes.loc[0])
	auto_routes.to_file("../out/alternative_routes/alternative_routes.geojson", driver='GeoJSON')

	print("Finish function generate_alternative_routes:",datetime.now())


# Функция генерации слоя плотности маршрутов
def generate_route_density():
	print("Start generate_route_density. Time:",datetime.now())
	routes_file = open("../out/routes/geojson/routes.geojson","r")
	routes = json.load(routes_file)

	# Словарь для хранения простых частей маршрутов и подсчёта плотности
	lines_dict = []

	for route in routes['features']:
		route_code = route['properties']['route_code']
		print("density, route_code: ",route_code)

		geometry = route['geometry']
		# Если тип 'LineString', то оборачиваем в массив, чтобы одинаково обрабатывать с MultyLineString
		if geometry['type'] == 'LineString':
			coords = [geometry['coordinates']]
		else:
			coords = geometry['coordinates']
		
		# Проходим в цикле по координатам каждой линии
		for points in coords:
			points_len = len(points)
			for i, point in enumerate(points):
				if i < points_len-1:
					line = [point,points[i+1]]

					# Ищем линию с такими же координатами
					dens_obj = list(filter(lambda x: x['line'] == line, lines_dict))
					# Если такой линии нет, то добавляем новую запись в список
					if len(dens_obj) == 0: 
						lines_dict.append({'line':line, 'routes':[route_code], 'field_2':1})
					# Если линия находится и маршрут ранее не был добавлен, то добавляем его в список
					elif route_code not in dens_obj[0]['routes']:
						dens_obj[0]['routes'].append(route_code)
						dens_obj[0]['field_2'] += 1
						

	# Конвертируем в объекты с shapely геометриями
	features = [] 
	for line in lines_dict:
		f = {
				'field_2': line['field_2'],
				'routes': json.dumps(line['routes'],ensure_ascii=False),
				'geometry': LineString(line['line'])
			}
		features.append(f)

	# Выгружаем в geojson
	os.system("mkdir ../out/density")
	geopandas.GeoDataFrame(features).to_file("../out/density/routes_density.geojson", driver='GeoJSON')
	
	routes_file.close()

	print("Finish generate_route_density. Time:",datetime.now())


# Функция расчёта слоя Расстояние между остановками
def calculate_stops_distance():
	routes_file = open("../out/routes/geojson/routes.geojson")
	routes = json.load(routes_file)['features']

	route2stops_file = open('../out/route2stops/route2stops.csv')
	route2stops = DictReader(route2stops_file,delimiter=";")

	stations_file = open('../out/stations/geojson/stations.geojson')
	stations = json.load(stations_file)['features']

	# Список фичей с сегментами маршрутов
	features = []
	
	# Первую станицию считаем отдельно
	r2s = next(route2stops)
	st_from_id = r2s['station_id']
	st_from = next(x for x in stations if x['properties']['global_id'] == st_from_id)
	st_from_geom = {'geometry': shape(st_from['geometry'])}
	st_from_route = r2s['route_id']
	st_from_track_no = int(r2s['track_no'])

	# Маршрут
	route_geom = next(x for x in routes if x['properties']['ID'] == st_from_route)['geometry']['coordinates']
	track_geom = shape({"type": "LineString", "coordinates":route_geom[st_from_track_no-1]})
	#print(route_geom)

	# Проходим в цикле по всем связям остановок с маршрутами
	for r2s in route2stops:
		#print(r2s['route_id'])

		# Получаем данные по следующей остановке
		st_to_id = r2s['station_id']
		st_to = next(x for x in stations if x['properties']['global_id'] == st_to_id)
		st_to_geom = {'geometry': shape(st_to['geometry'])}
		st_to_route = r2s['route_id']
		st_to_track_no = int(r2s['track_no'])
		
		# Вычисляем были такой сегмент уже в расчёте
		#seg_not_exists = next((x for x in features if st_from_id in x['station_ids'] and st_to_id in x['station_ids']),None) == None
		seg_not_exists = next((x for x in features if st_from_id+'-'+st_to_id in x['station_ids']),None) == None
		
		# Если остановки на одном маршруте, выполняем расчёт расстояния между ними
		if st_from_route == st_to_route and seg_not_exists == True:
			#Проверяем, что остановки на одном направлении
			if st_from_track_no == st_to_track_no:

				route_seg = turf.line_slice(st_from_geom, st_to_geom, track_geom)
				route_len = round(turf.get_line_length(route_seg)*1000)
				
				if route_len > 0:
					feature = {
						'station_ids':st_from_id+'-'+st_to_id,
						'station_from': st_from['properties']['StationName'],
						'station_to':st_to['properties']['StationName'],
						'distance': route_len,
						'geometry': route_seg
					}

					features.append(feature)
			else:
				track_geom = shape({"type": "LineString", "coordinates":route_geom[st_to_track_no-1]})
		# Если сменился маршрут, то получаем данные по новому маршруту
		else:
			route_geom = next(x for x in routes if x['properties']['ID'] == st_to_route)['geometry']['coordinates']
			track_geom = shape({"type": "LineString", "coordinates":route_geom[st_to_track_no-1]})

		# Переопредеяем первую остановку
		st_from_route = st_to_route
		st_from_track_no = st_to_track_no
		st_from_id = st_to_id
		st_from = st_to
		st_from_geom = st_to_geom

	# Закрываем файлы источники
	routes_file.close()
	route2stops_file.close()
	stations_file.close()

	# Записываем результат в файл
	os.system("mkdir ../out/stops_distance")
	geopandas.GeoDataFrame(features).to_file("../out/stops_distance/stops_distance.geojson", driver='GeoJSON')




# =========================== 
if __name__ == '__main__':

	print("Start", datetime.now())

	# Читаем параметры из файла параметров
	init()
	postgres = postgres.Postgres()

	#== Step 1: Загрузка данных по маршрутам и остановкам

	# # Step 1.1: Загрузка маршрутов и остановок
	# load_route_and_stations()
	
	# # Step 1.2: Выгрузка в формат geojson остановок и маршрутов
	# generate_stations_geojson()
	# generate_routes_geojson()

	# # Step 1.4: Загрузка данных в БД
	# postgres.upload_city()
	# postgres.upload_stations()
	# postgres.upload_routes()
	# postgres.upload_lnk_station_routes()

	# Step 1.5: Создание векторых файлов
	mapbox.create_stations_tileset(city_code)
	mapbox.create_routes_tileset(city_code)


	#== Step 2: Load isochrones
	stations = read_stations()
	isochrones.init()
	# Step 2.1: Запуск загрузки изохронов
	isochrones.generate_isochrones(city_code, "walking", stations, "w", "5,10,20,30")
	isochrones.generate_isochrones(city_code, "cycling", stations, "w", "10,20,30")
	isochrones.generate_isochrones(city_code, "driving", stations, "w", "10,20,30")
	isochrones.run_public_transport(stations)
	isochrones.generate_route_isochrones()
	isochrones.get_stops_cover_iso()

	# Step 2.2: Загрузка изохронов в БД
	postgres.upload_isochrones("walking")
	postgres.upload_isochrones("cycling")
	postgres.upload_isochrones("driving")
	postgres.upload_isochrones("public_transport")
	postgres.upload_isochrones("route_cover")

	# Step 2.3: Создаём вектор для покрытия остановками
	mapbox.create_stops_cover_tileset(city_code)


	#== Step 3: Расчёт плотности маршрутов
	generate_route_density()
	# Создаём вектор с плотностью
	mapbox.create_routes_density_tileset(city_code)


	#== Step 4: Расчёт расстояний между остановками
	calculate_stops_distance()
	# Создание вектора с расстояними между остановками
	mapbox.create_stops_distance_tileset(city_code)


	#== Step 5.1: Загрузка домов c реформы ЖКХ
	# Конвертируем файл в geoJson и добавляем координаты домов
	reforma.get_city_houses(city_code,city_name,region_name)
	
	# # Step 5.2: Расчёт домов далеко от остановок
	reforma.get_houses_far_from_stops()
	mapbox.create_houses_far_stops_tileset(city_code)
	postgres.upload_houses()


	# Step 6: Загрузка слоя с ДТП
	dtp.download_dtp(city_code,region_name)
	mapbox.create_dtp_map_tileset(city_code)

	# Загрузка очагов ДТП
	dtp.download_dtp_ochagi(city_code,region_name)
	mapbox.run_create_tileset(city_code,"dtp_ochagi")

	# Загрузка камер
	scraper.download_traffic_cameras(city_code,region_name)
	mapbox.run_create_tileset(city_code,"traffic_cameras")


	#== Step 7: Загрузка метрик

	# Step 7.1: Загрузка метрик для изохронов
	metrics.run_isochrone_metrics_in_threads("walking",threads_num)
	metrics.run_isochrone_metrics_in_threads("cycling",threads_num)
	metrics.run_isochrone_metrics_in_threads("driving",threads_num)
	metrics.run_isochrone_metrics_in_threads("public_transport",threads_num)
	metrics.run_isochrone_metrics_in_threads("route_cover",threads_num)

	# Step 7.2: Загрузка метрик для остановок
	metrics.generate_station_metrics(default_interval)

	# Step 7.3: Загрузка метрик для маршрутов
	generate_alternative_routes()
	metrics.generate_route_metrics(default_interval=10)

	# Step 7.4: Загрузка метрик для города 
	metrics.generate_city_metrics()

	# Step 7.5: Загрузка метрик в БД
	postgres.upload_metrics("walking")
	postgres.upload_metrics("cycling")
	postgres.upload_metrics("driving")
	postgres.upload_metrics("public_transport")
	postgres.upload_metrics("route_cover")
	postgres.upload_station_metrics()
	postgres.upload_route_metrics()
	postgres.upload_city_metrics()

	# 8. Добавляем папки IN и OUT в zip архив
	os.system("cd ..; zip -r "+city_code+"_IN in")
	os.system("cd ..; zip -r "+city_code+"_OUT out")

	# 9. Перемещаем zip файлы в архив
	os.system("cd "+arch_dir+"; mkdir "+city_code)
	os.system("mv ../"+city_code+"_IN.zip "+arch_dir+"/"+city_code+"/")
	os.system("mv ../"+city_code+"_OUT.zip "+arch_dir+"/"+city_code+"/")

	print("Finish", datetime.now())
