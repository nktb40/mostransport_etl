import pandas
import requests
import os
import json
import time
import copy
from datetime import datetime
import math
import shapely
import numpy as np
from shapely.geometry import *
from shapely.ops import split, substring, unary_union, nearest_points
import geopandas
import re
from geopy import distance
import multiprocessing 
import glob
from turfik import distance as turf_dist
from turfik import destination as turf_dest
from turfik import helpers as turf_helpers

# Ключи для MapBox
mapbox_key = "1271a705c49502b00213730c28f54f23"
mbPublicToken = 'pk.eyJ1Ijoibmt0YiIsImEiOiJjazhscjEwanEwZmYyM25xbzVreWMyYTU1In0.dcztuEUgjlhgaalrc_KLMw'
userName = 'nktb'

# Загрузка параметров из файла с параметрами
global_params = json.load(open("../in/params/params.json", 'r'))
keys = global_params.keys()

city_code = global_params['city_code']

if 'default_interval' in keys:
	default_interval = global_params['default_interval']
else:
	default_interval = 0

if 'threads_num' in keys:
	threads_num = global_params['threads_num']
else:
	threads_num = 1
	

print("Param city_code =",city_code)
print("Param default_interval =",default_interval)


# Функция загрузки Маршрутов, Остановок и их связей
def load_route_and_stations():
	print("Start function load_route_and_stations: ", datetime.now())

	src_files = glob.glob("../in/routes_and_stops/*.json")

	routes = geopandas.GeoDataFrame(columns=['city_code','id','route_number','type_of_transport','route_name','route_code','circular_flag','geometry'])
	stations = geopandas.GeoDataFrame(columns=['city_code','id','station_name','route_numbers','geometry'])
	route2stops = pandas.DataFrame(columns=['city_code','route_id','station_ids','route_code','route_type','track_no'])

	for file_name in src_files:
		file = json.load(open(file_name, 'r'))
		
		#print(file['result']['items'][0]['directions'][0]['platforms'][0].keys())
		#print(file['result']['items'][0]['directions'][0]['platforms'][1])

		item = file['result']['items'][0]

		# Загружаем данные по маршруту
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
		else:
			print("New route type: "+item['subtype'])

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
			'geometry': json.dumps(shapely.geometry.mapping(geometry), separators=(',',':'))
		}

		#print(route)
		routes = routes.append(route, ignore_index=True)

		# загружаем данные по остановкам
		for i_d, direction in enumerate(item['directions']):
			# Определяем код направления маршрута
			if direction['type'] == 'circular':
				if len(item['directions']) == 1:
					track_no = 0
				else:
					track_no = i_d+1
			elif direction['type'] == 'forward':
				track_no = 1
			elif direction['type'] == 'backward':
				track_no = 2

			# Определяем остановки
			for platform in direction['platforms']:

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
						'geometry': json.dumps(shapely.geometry.mapping(geometry), separators=(',',':'))
					}

					stations = stations.append(station, ignore_index=True)
				else:
					station = station.iloc[0]
					if route['route_code'] not in station['route_numbers']:
						station['route_numbers'].append(route['route_code'])

				# Загружаем связь между маршрутом и остановкой
				r2s = route2stops.query('route_id == @route_id and track_no == @track_no')
				
				if r2s.empty:
					r2s = {
							'route_id': route_id,
							'station_ids': [station_id],
							'route_code': route_code,
							'route_type': direction['type'],
							'track_no': track_no
					}
					route2stops = route2stops.append(r2s, ignore_index=True)
				else:
					r2s = r2s.iloc[0]
					if station_id not in r2s['station_ids']:
						r2s['station_ids'].append(station_id)
	
	routes['city_code'] = city_code
	routes.to_csv("../out/routes/csv/routes.csv", sep=";")
	
	stations['city_code'] = city_code
	stations['route_numbers'] = stations['route_numbers'].apply(lambda x: json.dumps(x,ensure_ascii=False))
	stations.to_csv("../out/stations/csv/stations.csv", sep=";")
	
	route2stops['city_code'] = city_code
	route2stops['station_ids'] = route2stops['station_ids'].apply(lambda x: json.dumps(x))
	route2stops.to_csv("../out/route2stops/route2stops.csv", sep=";")

	print("Finish function load_route_and_stations: ",datetime.now())

# Функция загрузки списка остановок
def read_stations():
	stations = pandas.read_csv("../out/stations/csv/stations.csv", sep=";")
	stations['geometry'] = stations['geometry'].apply(lambda x: shape(json.loads(x)))
	stations['id'] = stations['id'].astype(str)
	return stations

# Функция загрузки списка маршрутов
def read_routes():
	routes = pandas.read_csv("../out/routes/csv/routes.csv", sep=";")
	routes['geometry'] = routes['geometry'].apply(lambda x: shape(json.loads(x)))
	return routes

# Функция загрузки связей маршрутов и остановок
def read_route2stops():
	route2stops = pandas.read_csv('../out/route2stops/route2stops.csv', sep=";")
	route2stops['station_ids'] = route2stops['station_ids'].apply(json.loads)
	return route2stops

# Функция загрузки 5-ти минутных пеших изохронов
def read_walk_iso():
	walk_iso = pandas.read_csv("../out/isochrones/isochrones_walking.csv", sep=";").query('contour == 5')
	walk_iso['geometry'] = walk_iso['geometry'].apply(lambda x: shape(json.loads(x)))
	return walk_iso

# Функция генерации geojson файлов для остановок
def generate_stations_geojson():
	df_in = read_stations()[['id','city_code','station_name','route_numbers','geometry']]
	df_in['id'] = df_in['id'].astype(str)
	df_in['route_numbers'] = df_in['route_numbers'].apply(lambda x: "; ".join(json.loads(x)))
	df_in.rename(columns={"id": "global_id", "station_name":"StationName", "route_numbers":"RouteNumbers"}, errors="raise",inplace=True)
	
	df_in["tippecanoe"] = json.dumps({"layer" : "bus_stops"}, separators=(',',':'))
	df_out = geopandas.GeoDataFrame(df_in)
	df_out.to_file("../out/stations/geojson/stations.geojson", driver='GeoJSON')

# Функция генерации geojson файлов для маршрутов
def generate_routes_geojson():
	df_in = read_routes()[['id','city_code','route_name','route_code','geometry']]
	df_in['id'] = df_in['id'].astype(str)
	df_in.rename(columns={"id": "ID", "route_name":"RouteName"}, errors="raise",inplace=True)

	df_in["tippecanoe"] = json.dumps({"layer" : "routes"}, separators=(',',':'))
	df_out = geopandas.GeoDataFrame(df_in)
	df_out.to_file("../out/routes/geojson/routes.geojson", driver='GeoJSON')

# Генерация маршрутов по автомобильным доргам для расчётв прямолинейности
def generate_alternative_routes():
	routes = read_routes()
	routes.query('circular_flag == False', inplace=True)
	
	auto_routes = geopandas.GeoDataFrame(columns=['route_id','route_code','distance','geometry'])

	routes_len = len(routes)
	for i_r, route in routes.iterrows():
		print(str(i_r)+" from "+str(routes_len)+ " last: "+route['route_code'])

		directions = route.geometry
		direct_len = directions[0].length
		back_len = directions[1].length

		if direct_len > back_len:
			route_line = directions[0]
		else:
			route_line = directions[1]

		first_point = str(route_line.coords[0][0])+","+str(route_line.coords[0][1])
		last_point = str(route_line.coords[-1][0])+","+str(route_line.coords[-1][1])

		urlBase = 'https://api.mapbox.com/directions/v5/mapbox/driving/';
		query = urlBase +first_point+";"+last_point+'?geometries=geojson'+'&access_token=' + mbPublicToken
		headers = {'Content-type': 'Application/json', 'Accept': 'application/json'}
		result = send_request("get", query, '', headers)

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

# Функция для добавляения атрибутов к маршрутам
def get_routes_attributes():
	routes = read_routes()

	routes['route_length'] = routes.apply(lambda r: round(get_line_length(r['geometry']),2), axis=1)

	# Добавление интервалов
	if os.path.exists('../in/intervals/intervals.csv'):
		intervals = pandas.read_csv("../in/intervals/intervals.csv", sep=";", usecols=['location_code','route_id', 'route_code', 'avg_interval'])
		new_routes = pandas.merge(routes, intervals, how='inner', on='route_code')
		new_routes['route_cost'] = new_routes.apply(lambda r: round((60/r['avg_interval']*18-1)*r['route_length']*110), axis=1)
	elif math.isnan(default_interval) == False and default_interval != 0:
		new_routes = routes
		new_routes['avg_interval'] = default_interval
		new_routes['route_cost'] = new_routes.apply(lambda r: round((60/r['avg_interval']*18-1)*r['route_length']*110), axis=1)
	else:
		new_routes = routes
		new_routes['avg_interval'] = 0
		new_routes['route_cost'] = 0

	# Добавление коэф. прямолинейности
	alter_routes = geopandas.read_file("../out/alternative_routes/alternative_routes.geojson")
	new_routes = pandas.merge(new_routes, alter_routes, how='left', on='route_code')

	new_routes['straightness'] = new_routes.apply(lambda r: round(r['distance']/get_line_length(r['geometry_x'][0]),2), axis=1)
	new_routes['bbox'] = new_routes['geometry_x'].apply(lambda g: get_bbox(g))
	new_routes = new_routes[["city_code","id","route_code","avg_interval","route_length","route_cost","straightness","bbox"]]
	new_routes.rename(columns={"id": "route_id"}, errors="raise",inplace=True)
	new_routes['city_code'] = city_code
	
	new_routes.to_csv('../out/routes/csv/routes_info.csv', sep=";", columns=["city_code","route_id","route_code","avg_interval","route_length","route_cost","straightness","bbox"])

def get_bbox(geometry):
	b = shape(geometry).bounds 
	return [[b[0],b[1]],[b[2],b[3]]]

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
		ids = route2stops.query('route_id == @route_id')['station_ids'].tolist()
		if len(ids) > 0:
			ids = [y for x in ids for y in x]
			ids = list(set(ids))

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

# Функция получения изохронов через API MapBox
def get_iso(profile, lon, lat, times):
	#times = '10,20,30'
	urlBase = 'https://api.mapbox.com/isochrone/v1/mapbox';
	query = urlBase + '/' + profile + '/' + lon + ',' + lat + '?contours_minutes='+times+'&polygons=true&access_token=' + mbPublicToken
	headers = {'Content-type': 'Application/json', 'Accept': 'application/json'}
	data = ''
	result = send_request("get", query, data, headers)
	#print(result)
	return result

# Функция отправки запросов через API
def send_request(method, url, params, headers):
	#print("Sending request to:"+url)
	try:
		if method == "get":
			result = requests.get(url, params, headers=headers)
		elif method == "post":
			result = requests.post(url, data=json.dumps(params), headers=headers)
	except requests.exceptions.RequestException as e:  # This is the correct syntax
		print(e)
	return result.json()

# Герерация изохронов: Пеших, Вело и Авто
def generate_isochrones(profile, points, mode, times):
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
		iso = get_iso(profile, row['longitude'],row['latitude'],times)
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

# Функция обрезания линии по заданной длине
def line_slice_along(line, startDist, stopDist):
	coords = []
	slice = []
	options = {'units':'kilometers'}
	# Validation

	if line.type == 'Feature':
		coords = line.coords
	elif line.type == 'LineString':
		coords = line.coords

	origCoordsLength = len(coords)
	travelled = 0
	overshot = 0
	direction = 0
	interpolated = 0

	for i, coord in enumerate(coords):
		if startDist >= travelled and i == len(coords) - 1:
			break
		elif travelled > startDist and len(slice) == 0:
			overshot = startDist - travelled
			if overshot == 0:
				slice.append(coords[i])
				return LineString(slice)

			direction = bearing(coords[i], coords[i - 1]) - 180
			interpolated = turf_dest(coords[i], overshot, direction,options)
			slice.append(interpolated['geometry']['coordinates'])

		if travelled >= stopDist:
			overshot = stopDist - travelled
			if overshot == 0:
				slice.append(coords[i])
				return LineString(slice)

			direction = bearing(coords[i], coords[i - 1]) - 180
			interpolated = turf_dest(coords[i], overshot, direction,options)
			slice.append(interpolated['geometry']['coordinates'])
			return LineString(slice)

		if travelled >= startDist:
			slice.append(coords[i])

		if i == len(coords) - 1:
			return LineString(slice)
		
		travelled += turf_dist.distance(coords[i], coords[i + 1])

	return LineString(coords[coords.length - 1])

# Получение направления одной точки относительно другой в градусах
def bearing(start, end):
	coordinates1 = start
	coordinates2 = end

	lon1 = turf_helpers.degrees_to_radians(coordinates1[0])
	lon2 = turf_helpers.degrees_to_radians(coordinates2[0])
	lat1 = turf_helpers.degrees_to_radians(coordinates1[1])
	lat2 = turf_helpers.degrees_to_radians(coordinates2[1])
	a = math.sin(lon2 - lon1) * math.cos(lat2)
	b = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(lon2 - lon1)

	return turf_helpers.radians_to_degrees(math.atan2(a, b))

# Расчёт длины линии в км
def get_line_length(obj):
	len_res = 0

	if obj.type == 'LineString':
		lines = [obj]
	elif obj.type == 'MultiLineString':
		lines = obj

	for line in lines:
		coords = line.coords
		for i_c, c in enumerate(coords):
			if i_c+1 < len(coords):
				len_res += turf_dist.distance(c,coords[i_c+1])
	return len_res

# Расчёт площади фигуры в кв. м
def calculateArea(shape_obj):
	total = 0

	geom_type = shape_obj.geom_type

	if geom_type == 'Polygon':
		total = polygonArea(shape_obj)
	elif geom_type == 'MultiPolygon':
		for obj in shape_obj:
			total += polygonArea(obj)
	return total

# Расчёт площади полигона
def polygonArea(shape_obj):
	total = 0

	total += abs(ringArea(shape_obj.exterior.coords))

	for i in shape_obj.interiors:
			total -= abs(ringArea(i.coords))

	return total

# Расчёт площади по формуле turf.js
def ringArea(coords):
	#print(len(coords))
	RADIUS = 6378137 #earthRadius
	total = 0
	p1 = 0
	p2 = 0
	p3 = 0
	lowerIndex = 0
	middleIndex = 0
	upperIndex = 0
	coordsLength = len(coords)

	if coordsLength > 2:
		for i in range(0, coordsLength):
			if i == coordsLength - 2: # i = N-2
				lowerIndex = coordsLength - 2
				middleIndex = coordsLength - 1
				upperIndex = 0
			elif i == coordsLength - 1: # i = N-1
				lowerIndex = coordsLength - 1
				middleIndex = 0
				upperIndex = 1
			else: # i = 0 to N-3
				lowerIndex = i
				middleIndex = i + 1
				upperIndex = i + 2
			
			p1 = coords[lowerIndex]
			p2 = coords[middleIndex]
			p3 = coords[upperIndex]
			total += (rad(p3[0]) - rad(p1[0])) * math.sin(rad(p2[1]))
		
		total = total * RADIUS * RADIUS / 2
	return total

def rad(num):
	return num * math.pi / 180

def deduplicate(list):
	new_list = []
	for item in list:
		if item not in  new_list:
			new_list.append(item)
	return new_list

# Функция для нахождения соседних остановок в радиусе пешего 5-ти минутного изохрона
def generate_station_neighbors():

	print("Start function generate_station_neighbors: ", datetime.now())

	stations = read_stations()
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
			if geometry != None:
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
		connected_stops = get_stops_on_route_inside_isochrone(stop, stops, routes, route2stops, [], intervals, with_interval, times, [],0)

		# Добавляем связанные остановки с учётом пересадок
		if with_changes == "1":
			cp = copy.deepcopy(connected_stops)
			for c in cp: 
				cur_routes = stop['route_numbers'].split("; ")
				items = c['items']
				for i, item in enumerate(items):
					id = item['id']
					distance = item['distance']

					# Находим связанные остановки для пересадочных маршрутов без смены остановки
					con_stop = stops.query('id == @id').iloc[0]
					connected_stops = get_stops_on_route_inside_isochrone(con_stop, stops, routes, route2stops, cur_routes, intervals, with_interval, c['contour'], connected_stops, distance)
					route_nums = con_stop['route_numbers'].split("; ")
					cur_routes = list(set(route_nums + cur_routes))

					# Находим связанные остановки для пересадочних маршрутов со сменой остановки (соседние остановки)
					filtered_neibs = neighbors.query('station_id == @id')['neighbor_ids']
					if len(filtered_neibs) > 0:
						filtered_neibs = json.loads(filtered_neibs.iloc[0])
						for n_id in filtered_neibs:
							neib_stop = stops.query('id == @n_id').iloc[0]
							connected_stops = get_stops_on_route_inside_isochrone(neib_stop, stops, routes, route2stops, cur_routes, intervals, with_interval, c['contour'], connected_stops, distance+5)
							
							route_nums = neib_stop['route_numbers'].split("; ")
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
def get_stops_on_route_inside_isochrone(stop, stops, routes, route2stops, cur_routes, intervals, with_interval, times, connected_stops,dist_from_start_point):
	# Фильтруем маршруты
	route_nums = json.loads(stop['route_numbers'])
	route_nums = [item for item in route_nums if item not in cur_routes]

	if len(intervals) == 0:
		interval_from_default = True
	else:
		interval_from_default = False
	
	if len(route_nums) > 0:

		filtered_routes = routes.query('route_code in @route_nums')

		if len(filtered_routes) > 0:

			stop_id = stop['id']
			point = stop['geometry']

			# Фильтруем связь маршрутов со списком остановок  
			filtered_r2s = route2stops.copy()
			filtered_r2s['has_stop'] = filtered_r2s['station_ids'].apply(lambda x: stop_id in x)
			filtered_r2s.query('has_stop == True',inplace=True)

			# Находим остановки на пути выбранных маршрутов внутри изохрона
			for i_r, route in filtered_routes.iterrows():
				route_code = route['route_code']
				r2s = filtered_r2s.query('route_code == @route_code')
				#print("route_code: "+route_code)
				interval = 0
				if with_interval == "1":
					if interval_from_default == True:
						interval = default_interval/2
					else:
						interval_list = intervals.query('route_code == @route_code')['avg_fact_interval'].tolist()
						if len(interval_list) == 0:
							print("No interval for route: "+route_code)
						else:
							interval = round(int(interval_list[0])/2)

				route_coord = route['geometry']	

				for i_d, direction in  r2s.iterrows():
					
					stops_ids = direction['station_ids']
					dir_type = direction['route_type']

					if len(route_coord) == 1:
						line = route_coord[0]
					elif dir_type == 'forward':
						line = route_coord[0]
					elif dir_type == 'backward':
						line = route_coord[1]
					elif dir_type == 'circular':
						st_pt = stops[stops['ID'] == direction['station_ids'][0]].iloc[0]
						st_pt_coord = list(st_pt.geometry.coords)[0]
						st_coord_1 = list(route_coord[0].coords)[0]
						st_coord_2 = list(route_coord[1].coords)[0]

						len1 = get_line_length(LineString([st_pt_coord,st_coord_1]))
						len2 = get_line_length(LineString([st_pt_coord,st_coord_2]))

						if len1 > len2:
							line = route_coord[0]
						else:
							line = route_coord[1]

					# Строим линию направления
					multi_point = MultiPoint(line.coords)

					# Определяем близжайшую к остановке точку на маршруте
					pointOnRoute = nearest_points(multi_point,point)[0]
					
					# Находим точки остановок для маршрута
					stopsOnRoute = stops.copy()
					stopsOnRoute = stopsOnRoute.query('id in @stops_ids')

					# Определяем близжайшую точку на маршруте для всех остановок направления
					stopsOnRoute['pointOnRoute'] = stopsOnRoute.apply(lambda row: nearest_points(multi_point,row['geometry'])[0], axis=1)

					# Проверяем, что выбранная остановка не является последней на направлении
					if stop_id == stops_ids[-1]:
						line = pointOnRoute
					else:
						# Обрезаем линию от остановки до конца маршрута
						line = split(line, pointOnRoute)[-1]

					# Нарезаем маршруты по длине, соответствующей времени пути
					for t in times.split(','):
						# Время пути = время изохрона минус интервал ожидания минус время он от выбранной точки
						t_modif = int(t) - interval - dist_from_start_point

						if t_modif > 0:
							start_dist = 0
							stop_dist = 15 * t_modif / 60
							if line.type == "Point":
								sliced = line
							else:
								sliced = line_slice_along(line,start_dist,stop_dist)

							# Определяем точки, которые находятся внутри маршрута заданной длины
							stopsOnRoute['on_time_flag'] = stopsOnRoute.apply(lambda row: row['pointOnRoute'].intersects(sliced), axis=1)
							stopsOnTime = stopsOnRoute.query('on_time_flag == True')

							if len(stopsOnTime) > 0:
								stopsOnTime = stopsOnTime.assign(distance=np.nan)

								# ===============
								if dist_from_start_point == 0:
									sliced_points = MultiPoint(sliced.coords)

									stopsOnTime['line_index'] = stopsOnTime['pointOnRoute'].apply(lambda stp: next((i for i, x in enumerate(sliced_points) if x == stp),-1))
									stopsOnTime.sort_values(by="line_index",inplace=True)
									stopsOnTime['distance'] = stopsOnTime['line_index'].apply(lambda ind: round(get_line_length(LineString(sliced_points[0:ind+1]))/15*60) if ind > 0 else 0)
									stopsOnTime.set_index('id',inplace=True, drop=False)
								
								stopsOnTime['items'] = stopsOnTime.apply(lambda x: {'id':x['id'], 'distance':x['distance']},axis=1)
								# ===============

								contour = [c for c in connected_stops if c['contour'] == t]
								ids = stopsOnTime['id'].tolist()
								items = stopsOnTime['items'].tolist()
								
								# Добавляем ID остановок каждого контура в свой массив
								if contour == []:
									connected_stops.append({'contour':t, 'ids':ids, 'items':items, 'routes':[route_code]})
								else:
									contour = contour[0]
									
									contour['ids'] = list(set(contour['ids'] + ids))
									contour['routes'] = list(set(contour['routes'] + [route_code]))
									if dist_from_start_point == 0:
										contour['items'] = deduplicate(contour['items'] + items)
	return connected_stops

# Функция генерации метрик для изохронов
def generate_isohrone_metrics(isochrone_df, profile, part):
	# Файл для записи результата работы функции
	file_out = open("../out/metrics/tmp/metrics_"+profile+"_"+part+".csv", "w")
	file_out.write("metric_code;isochrone_code;metric_value\n")

	# Загружаем в DataFrame-ы источники данных
	df_sources = []
	for f in glob.glob("../in/objects/*.csv"):
		df_sources.append(geopandas.read_file("out/"+f+"/"+f+".geojson"))

	df_len = len(isochrone_df)
	ind = 0

	# Выполняем расчёт метрик для каждого изохрона
	for i, row in isochrone_df.iterrows():
		ind += 1
		iso_id = row['id']
		print("Metrics. "+profile+". Part: "+str(part)+". Row: "+str(ind)+" from: "+str(df_len)+". Last: "+iso_id)
		geom = shape(json.loads(row['geometry']))
		bounds = geom.bounds

		if len(bounds) > 0:
			# Площадь изохрона
			isochrone_area = round(calculateArea(geom)/1000000,2)
			file_out.write("isochrone_area;"+iso_id+";"+str(isochrone_area)+"\n")
			
			# Метрики из списка метрик
			for s in df_sources:
				info = get_objects_inside_info(s,bounds,geom)
				if info["cnt"] > 0:
					file_out.write(s+"_cnt;"+iso_id+";"+str(info["cnt"])+"\n")
				if info["population"] > 0:
					file_out.write(s+"_population;"+iso_id+";"+str(info["population"])+"\n")

	file_out.close()
	
def get_objects_inside_info(df,bounds,geometry):
	#print(bounds)
	#print(df)
	in_bounds = df.cx[bounds[0]:bounds[2], bounds[1]:bounds[3]]

	if len(in_bounds) > 0:
		in_bounds['in_polygon_flag'] = in_bounds.apply(lambda p: p['geometry'].within(geometry), axis=1)
		in_polygon = in_bounds.query('in_polygon_flag == True')
		points_cnt = len(in_polygon)
		population = in_polygon['population'].agg('sum')
	else:
		points_cnt = 0
		population = 0
	return {"cnt":points_cnt, "population":population}

# Запуск расчёта изохронов в многопоточном режиме
def run_public_transport_in_threads(df, times, with_interval, with_changes):
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

# Запуск расчёта метрик в многопоточном режиме
def run_metrics_in_threads(df, profile):
	print("Start gen metrics for: "+profile+". Time:",datetime.now())
	df_split = np.array_split(df, threads_num)
	processes = []
	for i in range(0,threads_num):
		print("Metrics. Part: "+str(i+1)+" len: "+str(len(df_split[i])))
		p = multiprocessing.Process(target=generate_isohrone_metrics, args=(df_split[i], profile, str(i+1)))
		processes.append(p)
		p.start()

	for process in processes:
		process.join()

	out_files = glob.glob("../out/metrics/tmp/*.csv")
	out_df = pandas.DataFrame()
	for file_name in out_files:
	 	out_df = out_df.append(pandas.read_csv(file_name, sep=";"), sort=False)
	 	os.remove(file_name)

	out_df.to_csv("../out/metrics/metrics_"+profile+".csv", sep=";")
	print("Finish gen metrics for: "+profile+". Time:",datetime.now())
	    
# =========================== 
print("Start", datetime.now())

#== Step 1: Загрузка данных по маршрутам и остановкам

# Step 1.1: Загрузка маршрутов и остановок
load_route_and_stations()

# Step 1.2: Загрузка альтернативных маршрутов
generate_alternative_routes()

# Step 1.3: Загрузка доп. информации о маршрутах
get_routes_attributes()

# Step 1.4: Выгрузка geojson для остановок
generate_stations_geojson()

# Step 1.5: Выгрузка geojson для маршрутов
generate_routes_geojson()


#== Step 2: Load isochrones
stations = read_stations() #.query('id == "12385581875069176"')

# Step 2.1: Load Walking isochrones
generate_isochrones("walking", stations, "w", "5,10,20,30")

# Step 2.2: Load Cycling isochrones
generate_isochrones("cycling", stations, "w", "10,20,30")

# Step 2.3: Load Driving isochrones
generate_isochrones("driving", stations, "w", "10,20,30")

# Step 2.4: Загружаем изохроны Public transport

# Step 2.4.1: Изохроны без интервалов и пересадок
run_public_transport_in_threads(stations, "10,20,30", "0", "0")

# Step 2.4.2: Находим соседние остановки для всех остановок
generate_station_neighbors()

#Step 2.4.3: Изохроны без интервалов, но с пересадками
run_public_transport_in_threads(stations, "10,20,30", "0", "1")

# Step 2.4.4: Изохроны с интервалами, но без пересадок
if (os.path.exists('../in/intervals/intervals.csv')) or (math.isnan(default_interval) == False and default_interval != 0):
	run_public_transport_in_threads(stations, "10,20,30", "1", "0")

# Step 2.4.5: Изохроны с интервалами, и с пересадками
if (os.path.exists('../in/intervals/intervals.csv')) or (math.isnan(default_interval) == False and default_interval != 0):
	run_public_transport_in_threads(stations, "10,20,30", "1", "1")

# Step 2.4.6: Загрузка изохронов маршрутов
generate_route_isochrones()

#== Step 3: Загрузка метрик

# Step 3.1: Загрузка метрик для пеших изохронов
iso_df = pandas.read_csv("../out/isochrones/isochrones_walking.csv", sep=";")
run_metrics_in_threads(iso_df, "walking")

# Step 3.2: Загрузка метрик для велосипедных изохронов
iso_df = pandas.read_csv("../out/isochrones/isochrones_cycling.csv", sep=";")
run_metrics_in_threads(iso_df, "cycling")

# Step 3.3: Загрузка метрик для автомобильных изохронов
iso_df = pandas.read_csv("../out/isochrones/isochrones_driving.csv", sep=";")
run_metrics_in_threads(iso_df, "driving")

# Step 3.4: Загрузка метрик для изохронов ОТ

isochrone_files = glob.glob("../out/isochrones/public_transport/*.csv")

for i, file_name in enumerate(isochrone_files):
	iso_df = pandas.read_csv(file_name, sep=";")
	run_metrics_in_threads(iso_df, "public_transport-"+str(i+1))

print("Finish", datetime.now())