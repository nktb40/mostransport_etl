import psycopg2 as pg
import json
from csv import DictReader 
from datetime import datetime
import glob
import mapbox
import math
import numpy

# Подключение к БД
def get_connection():
	# conn = pg.connect(
	# 	host="localhost",
	# 	database="mostransport_development",
	# 	user="mostransport",
	# 	password="password"
	# )
	conn = pg.connect(
		host="84.201.146.240",
		database="mostransport",
		user="mostransport",
		password="mostranspass"
	)
	return conn

# Получение данных из БД
def send_query(query):
	conn = get_connection()
	try:
		cursor = conn.cursor()
		cursor.execute(query)
		result = cursor.fetchone()
		return result
	except (Exception, pg.DatabaseError) as error:
		print(error)
	finally:
		conn.close()

# Функция добавления города БД
def upload_city():
	print("Start upload_city")
	with open("../in/params/params.json", 'r') as file:
		city_params = json.load(file)

	city_code = city_params['city_code']
	city_name = city_params['city_name']
	region_name = city_params['region_name']
	area = city_params['area']
	population = city_params['population']

	location = mapbox.get_location(city_name)['features'][0]
	bbox = location['bbox']
	longitude = location['center'][0]
	latitude = location['center'][1]

	# Составляем запрос
	q = "INSERT INTO cities (name,code,region_name,longitude,latitude,bbox,area,population,created_at,updated_at) VALUES"
	q += "('"+city_name+"','"+city_code+"','"+region_name+"',"+str(longitude)+","+str(latitude)+",'"+str(bbox)+"',"+str(area)+","+str(population)+",NOW(),NOW())"
	q += "\nON CONFLICT (code)"
	q += ("\nDO UPDATE SET "+
		 	"name=EXCLUDED.name,region_name=EXCLUDED.region_name,longitude=EXCLUDED.longitude,latitude=EXCLUDED.latitude,bbox=EXCLUDED.bbox,area=EXCLUDED.area,population=EXCLUDED.population,updated_at=NOW();")

	q += "\ncommit;"

	# Отправялем запрос на вставку
	send_query(q)

	print("Finish upload_city")

# Функция загрузки остановок в БД
def upload_stations(city_code):
	print("Start upload_stations")
	# Получаем ID города в БД
	city_id = send_query("select id from cities where code='"+city_code+"'")[0]

	# Составляем запрос
	q = "INSERT INTO stations (source_id,latitude,longitude,route_numbers,station_name,geo_data,city_id,created_at,updated_at)"
	q += "\nselect source_id, latitude, longitude, route_numbers, station_name, cast(geo_data as JSON) as geo_data, "+str(city_id)+" as city_id, NOW(), NOW() from ("

	with open("../out/stations/csv/stations.csv","r") as file:
		reader = DictReader(file, delimiter=';')
		for row in reader:

			q += ("\nselect '"+row['id']+"' as source_id, "+row['latitude']+" as latitude, "+row['longitude']+" as longitude, '"+
				  row['route_numbers']+"' as route_numbers, '"+row['station_name']+"' as station_name, '"+
				  row['geometry']+"' as geo_data union all")

	q = q.rstrip(" union all")
	q += "\n) src" 
	q += "\nON CONFLICT (source_id,city_id)"
	q += ("\nDO UPDATE SET "+
		  "latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude, route_numbers=EXCLUDED.route_numbers, station_name=EXCLUDED.station_name, geo_data=EXCLUDED.geo_data, updated_at=NOW();")
	q += "\ncommit;"

	# Отправялем запрос на вставку
	send_query(q)
	print("Finish upload_stations")


# Функция загрузки маршрутов в БД
def upload_routes(city_code):
	print("Start upload_routes")
	# Получаем ID города в БД
	city_id = send_query("select id from cities where code='"+city_code+"'")[0]

	# Составляем запрос
	q = "INSERT INTO routes (route_number,route_code,route_name,type_of_transport,geo_data,source_id,circular_flag,bbox,city_id,created_at,updated_at)"
	q += "\nselect route_number,route_code,route_name,type_of_transport,cast(geo_data as JSON) as geo_data,source_id,cast(circular_flag as boolean) as circular_flag, cast(bbox as JSON) as bbox, "+str(city_id)+" as city_id, NOW(), NOW() from ("

	with open("../out/routes/csv/routes.csv","r") as file:
		reader = DictReader(file, delimiter=';')
		for row in reader:

			q += ("\nselect '"+row['id']+"' as source_id, '"+row['route_number']+"' as route_number, '"+row['type_of_transport']+"' as type_of_transport, '"+
				  row['route_name']+"' as route_name, '"+row['route_code']+"' as route_code, "+row['circular_flag']+" as circular_flag, '"+
				  row['geometry']+"' as geo_data, '"+row['bbox']+"' as bbox union all")

	q = q.rstrip(" union all")
	q += "\n) src" 
	q += "\nON CONFLICT (source_id,city_id)"
	q += ("\nDO UPDATE SET "+
		  "route_number=EXCLUDED.route_number, route_code=EXCLUDED.route_code, route_name=EXCLUDED.route_name, type_of_transport=EXCLUDED.type_of_transport, geo_data=EXCLUDED.geo_data, circular_flag=EXCLUDED.circular_flag, bbox=EXCLUDED.bbox, updated_at=NOW();")
	q += "\ncommit;"

	# Отправялем запрос на вставку
	send_query(q)
	print("Finish upload_routes")


# # Функция загрузки доп. информации о маршрутах
# def upload_routes_info(city_code):
# 	print("Start upload_routes_info")
# 	# Получаем ID города в БД
# 	city_id = send_query("select id from cities where code='"+city_code+"'")[0]

# 	# Составляем запрос
# 	q = "INSERT INTO routes (source_id, route_code, route_interval, route_length, route_cost, straightness, bbox, city_id, created_at, updated_at)"
# 	q += "\nselect source_id, route_code, route_interval, route_length, route_cost, straightness, cast(bbox as JSON) bbox, "+str(city_id)+" as city_id, NOW(), NOW() from ("

# 	with open("../out/routes/csv/routes_info.csv","r") as file:
# 		reader = DictReader(file, delimiter=';')
# 		for row in reader:
# 			row['straightness'] = 'NULL' if row['straightness'] == '' else row['straightness']

# 			q += ("\nselect '"+row['route_id']+"' as source_id, '"+row['route_code']+"' as route_code, "+row['avg_interval']+" as route_interval, "+
# 				  row['route_length']+" as route_length, "+row['route_cost']+" as route_cost, "+row['straightness']+" as straightness, '"+
# 				  row['bbox']+"' as bbox union all")

# 	q = q.rstrip(" union all")
# 	q += "\n) src" 
# 	q += "\nON CONFLICT (source_id,city_id)"
# 	q += ("\nDO UPDATE SET "+
# 		  "route_code=EXCLUDED.route_code, route_interval=EXCLUDED.route_interval, route_length=EXCLUDED.route_length, route_cost=EXCLUDED.route_cost, straightness=EXCLUDED.straightness, bbox=EXCLUDED.bbox, updated_at=NOW();")
# 	q += "\ncommit;"

# 	# Отправялем запрос на вставку
# 	send_query(q)
# 	print("Finish upload_routes_info")

# Функция загрузки связей маршрутов и остановок в БД
def upload_lnk_station_routes(city_code):
	print("Start upload_lnk_station_routes")
	# Получаем ID города в БД
	city_id = send_query("select id from cities where code='"+city_code+"'")[0]

	# Составляем запрос
	q = "INSERT INTO lnk_station_routes (station_id,route_id,route_type,track_no,seq_no,route_time,distance,created_at,updated_at)"
	q += "\nselect s.id as station_id, r.id as route_id, r2s.route_type, r2s.track_no, r2s.seq_no, r2s.route_time, r2s.distance, NOW(),NOW() from ("

	with open("../out/route2stops/route2stops.csv","r") as r2s_file:
		reader = DictReader(r2s_file, delimiter=';')
		for row in reader:

			row['route_time'] = 'NULL' if row['route_time'] == '' else row['route_time']
			row['distance'] = 'NULL' if row['distance'] == '' else row['distance']

			q += ("\nselect '"+row['station_id']+"' as station_id, '"+row['route_id']+"' as route_id, '"+row['route_type']+"' as route_type, "+
				  row['track_no']+" as track_no, "+row['seq_no']+" as seq_no, "+row['route_time']+" as route_time, "+
				  row['distance']+" as distance union all")

	q = q.rstrip(" union all")
	q += "\n) r2s" 
	q += "\nINNER JOIN stations s ON s.source_id = r2s.station_id AND s.city_id = "+str(city_id)
	q += "\nINNER JOIN routes r ON r.source_id = r2s.route_id AND r.city_id = "+str(city_id)
	q += "\nON CONFLICT (station_id,route_id,track_no)"
	q += ("\nDO UPDATE SET "+
		  "route_type=EXCLUDED.route_type, seq_no=EXCLUDED.seq_no, route_time=EXCLUDED.route_time, distance=EXCLUDED.distance, updated_at=NOW();")
	q += "\ncommit;"

	# Отправялем запрос на вставку
	send_query(q)
	print("Finish upload_lnk_station_routes")


# Функция загрузки изохронов в БД
def upload_isochrones(city_code, profile):
	print("Start upload_isochrones",profile)
	# Получаем ID города в БД
	city_id = send_query("select id from cities where code='"+city_code+"'")[0]

	# Находим пути к файлам с изохронами
	paths = []
	if profile == 'public_transport':
		paths = glob.glob("../out/isochrones/public_transport/*.csv")
	else:
		paths.append("../out/isochrones/isochrones_"+profile+".csv")

	for path in paths:
		print('load',path) 	

		# Составляем запрос
		q = "INSERT INTO isochrones (station_id,source_station_id,route_id,source_route_id,unique_code,contour,profile,with_interval,with_changes,geo_data,properties,city_id,created_at,updated_at)"
		
		q += ("\nselect s.id as station_id, src.source_station_id, r.id as route_id, src.source_route_id, src.unique_code, "+
			  "src.contour, src.profile, cast(src.with_interval as boolean) as with_interval, "+
			  "cast(src.with_changes as boolean) as with_changes,cast(src.geo_data as JSON) as geo_data, "+
			  "cast(src.properties as JSON) as properties,"+str(city_id)+" as city_id, NOW(),NOW() from (")

		with open(path,"r") as file:
			reader = DictReader(file, delimiter=';')
			for row in reader:
				source_station_id = 'NULL' if row['station_id'] == '' else "'"+row['station_id']+"'"
				source_route_id = 'NULL' if 'route_id' not in row else "'"+row['route_id']+"'"
				with_interval = 'NULL' if 'with_interval' not in row else row['with_interval']
				with_changes = 'NULL' if 'with_changes' not in row else row['with_changes']
				properties = 'NULL' if 'properties' not in row else "'"+row['properties']+"'"

				q += ("\nselect '"+row['id']+"' as unique_code, "+row['contour']+" as contour, '"+row['profile']+"' as profile, "+
					  source_station_id+" as source_station_id, "+source_route_id+" as source_route_id, '"+row['geometry']+"' as geo_data, "+with_interval+" as with_interval, "+
					  with_changes+" as with_changes, "+properties+" as properties union all")

		q = q.rstrip(" union all")
		q += "\n) src" 
		q += "\nLEFT JOIN stations s ON s.source_id = src.source_station_id AND s.city_id = "+str(city_id)
		q += "\nLEFT JOIN routes r ON r.source_id = src.source_route_id AND r.city_id = "+str(city_id)
		q += "\nON CONFLICT (unique_code)"
		q += ("\nDO UPDATE SET "+
			  "station_id=EXCLUDED.station_id, source_station_id=EXCLUDED.source_station_id, "+
			  "contour=EXCLUDED.contour, profile=EXCLUDED.profile, with_interval=EXCLUDED.with_interval, "+
			  "with_changes=EXCLUDED.with_changes,geo_data=EXCLUDED.geo_data, "+
			  "properties=EXCLUDED.properties,city_id=EXCLUDED.city_id, updated_at=NOW();")
		q += "\ncommit;"

		#print(q)
		# Отправялем запрос на вставку
		send_query(q)
	print("Finish upload_isochrones",profile)

# Функция загрузки метрик в БД
def upload_metrics(profile):
	print("Start upload_metrics",profile)
	# Находим пути к файлам с изохронами
	paths = []
	if profile == 'public_transport':
		paths = glob.glob("../out/metrics/metrics_public_transport*.csv")
	else:
		paths.append("../out/metrics/metrics_"+profile+".csv")

	for path in paths:
		print('load',path) 

		# Загружаем метрики в массив
		rows = []
		with open(path,"r") as file:
			reader = DictReader(file, delimiter=';')
			for row in reader:
				rows.append(row)

		# делим массив на чанки по 1000 записей
		for chunk in numpy.array_split(rows,math.ceil(len(rows)/1000)):
			# Составляем запрос
			q = "INSERT INTO metrics (metric_type_id,isochrone_id,isochrone_unique_code,metric_value,created_at,updated_at)"
			q += "\nselect m.id as metric_type_id, i.id as isochrone_id, src.isochrone_unique_code, src.metric_value, NOW(),NOW() from ("

			for row in chunk:
				q += ("\nselect '"+row['metric_code']+"' as metric_code, '"+row['isochrone_code']+"' as isochrone_unique_code, "+
					row['metric_value']+" as metric_value union all")
			
			q = q.rstrip(" union all")
			q += "\n) src" 
			q += "\nLEFT JOIN metric_types m ON m.metric_code = src.metric_code"
			q += "\nLEFT JOIN isochrones i ON i.unique_code = src.isochrone_unique_code"
			q += "\nON CONFLICT (metric_type_id, isochrone_id)"
			q += ("\nDO UPDATE SET "+
				  "metric_type_id=EXCLUDED.metric_type_id,isochrone_id=EXCLUDED.isochrone_id, isochrone_unique_code=EXCLUDED.isochrone_unique_code, metric_value=EXCLUDED.metric_value, updated_at=NOW();")
			q += "\ncommit;"
			#print(q)
			# Отправялем запрос на вставку
			send_query(q)
	print("Finish upload_metrics",profile)

# Функция загрузки метрик  отсановок в БД
def upload_station_metrics():
	print("Start upload_station_metrics")
	# Составляем запрос
	q = "INSERT INTO station_metrics (metric_type_id,station_id,metric_value,created_at,updated_at)"
	q += "\nselect m.id as metric_type_id, s.id as station_id, src.metric_value, NOW(),NOW() from ("

	with open("../out/metrics/station_metrics.csv","r") as file:
		reader = DictReader(file, delimiter=';')
		for row in reader:
			q += ("\nselect '"+row['metric_code']+"' as metric_code, '"+row['station_id']+"' as station_id, "+
				row['metric_value']+" as metric_value union all")
	
	q = q.rstrip(" union all")
	q += "\n) src" 
	q += "\nLEFT JOIN metric_types m ON m.metric_code = src.metric_code"
	q += "\nLEFT JOIN stations s ON s.source_id = src.station_id"
	q += "\nON CONFLICT (metric_type_id, station_id)"
	q += ("\nDO UPDATE SET "+
		  "metric_type_id=EXCLUDED.metric_type_id,station_id=EXCLUDED.station_id, metric_value=EXCLUDED.metric_value, updated_at=NOW();")
	q += "\ncommit;"

	# Отправялем запрос на вставку
	send_query(q)
	print("Finish upload_station_metrics")

# Функция загрузки метрик маршрутов в БД
def upload_route_metrics():
	print("Start upload_route_metrics")

	q = "INSERT INTO route_metrics (metric_type_id,route_id,metric_value,created_at,updated_at)"
	q += "\nselect m.id as metric_type_id, r.id as route_id, src.metric_value, NOW(),NOW() from ("

	with open("../out/metrics/route_metrics.csv","r") as file:
		reader = DictReader(file, delimiter=';')
		for row in reader:
			q += ("\nselect '"+row['metric_code']+"' as metric_code, '"+row['route_id']+"' as route_id, "+
				row['metric_value']+" as metric_value union all")

	q = q.rstrip(" union all")
	q += "\n) src" 
	q += "\nLEFT JOIN metric_types m ON m.metric_code = src.metric_code"
	q += "\nLEFT JOIN routes r ON r.source_id = src.route_id"
	q += "\nON CONFLICT (metric_type_id, route_id)"
	q += ("\nDO UPDATE SET "+
		  "metric_type_id=EXCLUDED.metric_type_id, route_id=EXCLUDED.route_id, metric_value=EXCLUDED.metric_value, updated_at=NOW();")
	q += "\ncommit;"

	# Отправялем запрос на вставку
	send_query(q)
	print("Finish upload_route_metrics")


# Функция загрузки метрик городов в БД
def upload_city_metrics():
	print("Start upload_city_metrics")

	q = "INSERT INTO city_metrics (metric_type_id,city_id,metric_value,created_at,updated_at)"
	q += "\nselect m.id as metric_type_id, c.id as city_id, src.metric_value, NOW(),NOW() from ("

	with open("../out/metrics/city_metrics.csv","r") as file:
		reader = DictReader(file, delimiter=';')
		for row in reader:
			q += ("\nselect '"+row['metric_code']+"' as metric_code, '"+row['city_code']+"' as city_code, "+
				row['metric_value']+" as metric_value union all")

	q = q.rstrip(" union all")
	q += "\n) src" 
	q += "\nLEFT JOIN metric_types m ON m.metric_code = src.metric_code"
	q += "\nLEFT JOIN cities c ON c.code = src.city_code"
	q += "\nON CONFLICT (metric_type_id, city_id)"
	q += ("\nDO UPDATE SET "+
		  "metric_type_id=EXCLUDED.metric_type_id, city_id=EXCLUDED.city_id, metric_value=EXCLUDED.metric_value, updated_at=NOW();")
	q += "\ncommit;"

	# Отправялем запрос на вставку
	send_query(q)
	print("Finish upload_city_metrics")

# Функция загрузки домов в БД
def upload_houses(city_code):
	print("Start upload_houses",city_code)
	# Получаем ID города в БД
	city_id = send_query("select id from cities where code='"+city_code+"'")[0]

	# Загружаем из файла данные о домах 
	with open('../out/houses/houses.geojson', 'r') as h_file:
		houses = json.load(h_file)['features']

	# Загружаем из файла данные о домах далекто от остановок
	with open('../out/houses_far_stops/houses_far_stops.geojson', 'r') as f_file:
		far_houses = list(map(lambda x: x['properties']['source_id'],json.load(f_file)['features']))

	# Составляем запрос 
	q = ("INSERT INTO houses(city_id,source_id,street_name,house_number,building,"+
		 "block,letter,address,floor_count_min,floor_count_max,entrance_count,"+
		 "area_total,area_residential,population,geometry,far_from_stops_flag,created_at,updated_at) VALUES")

	for house in houses:
		#print(house)
		h = house['properties']
		
		floor_count_min = h['floor_count_min'] if h['floor_count_min'] != '' else 'NULL'
		floor_count_max = h['floor_count_max'] if h['floor_count_max'] != '' else 'NULL'
		entrance_count = h['entrance_count'] if h['entrance_count'] != '' else 'NULL'
		area_total = h['area_total'].replace(',','.') if h['area_total'] != '' else 'NULL'
		area_residential = h['area_residential'].replace(',','.') if h['area_residential'] != '' else 'NULL'
		geometry = json.dumps(house['geometry'])
		far_flag = 'true' if h['source_id'] in far_houses else 'false'

		q += ("\n("+str(city_id)+",'"+h['source_id']+"','"+h['street_name']+"','"+h['house_number']+"','"+h['building']+"','"+
			  h['block']+"','"+h['letter']+"','"+h['address']+"',"+floor_count_min+","+floor_count_max+","+
			  entrance_count+","+area_total+","+area_residential+","+str(h['population'])+",'"+geometry+"',"+far_flag+",NOW(),NOW()),")

	q = q.rstrip(",")

	q += "\nON CONFLICT (city_id,source_id)"

	q += ("\nDO UPDATE SET "+
		 	"street_name=EXCLUDED.street_name,house_number=EXCLUDED.house_number,building=EXCLUDED.building,"+
		 	"block=EXCLUDED.block,letter=EXCLUDED.letter,address=EXCLUDED.address,floor_count_min=EXCLUDED.floor_count_min,"+
		 	"floor_count_max=EXCLUDED.floor_count_max,entrance_count=EXCLUDED.entrance_count,area_total=EXCLUDED.area_total,"+
		 	"area_residential=EXCLUDED.area_residential,population=EXCLUDED.population,geometry=EXCLUDED.geometry,far_from_stops_flag=EXCLUDED.far_from_stops_flag,updated_at=NOW();")

	q += "\ncommit;"
	
	# Отправялем запрос на вставку
	send_query(q)
	print("Finish upload_houses",city_code)

# Функция добавления нового слоя для города
def upload_layer(city_code,layer_type_code):
	print("Start upload_layer",city_code,layer_type_code)
	# Получаем ID города в БД
	city_id = send_query("select id from cities where code='"+city_code+"'")[0]
	# Получаем ID типа слоя в БД
	layer_type_id = send_query("select id from layer_types where source_name='"+layer_type_code+"'")[0]

	# Составляем запрос 
	q = ("INSERT INTO layers(city_id,layer_type_id,tile_url,created_at,updated_at) VALUES"+
		"("+str(city_id)+","+str(layer_type_id)+", 'nktb."+city_code+"-"+layer_type_code+"', NOW(),NOW())")
	q += "\nON CONFLICT (city_id,layer_type_id)"
	q += ("\nDO UPDATE SET "+
		 	"city_id=EXCLUDED.city_id,layer_type_id=EXCLUDED.layer_type_id,tile_url=EXCLUDED.tile_url,updated_at=NOW();")

	q += "\ncommit;"

	# Отправялем запрос на вставку
	send_query(q)
	print("Finish upload_layer",city_code,layer_type_code)


# ===============================================
if __name__ == '__main__':
	
	print("Begin postgres")
	#upload_houses('OMS')
	#upload_station_metrics()
	#upload_metrics("route_cover")
	#upload_station_metrics()
	#upload_route_metrics()
	#upload_city_metrics()
	#upload_routes("TUL")
	#upload_isochrones("TUL", "route_cover")
	#upload_lnk_station_routes("TUL")
	#upload_route_metrics()
	#upload_metrics("route_cover")