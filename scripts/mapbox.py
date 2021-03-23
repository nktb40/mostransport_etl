# Блок функций для создания Обращения к API Mapbox
import os
import time
import json
import requests
import postgres

# Ключи для MapBox
mapbox_key = "1271a705c49502b00213730c28f54f23"
mbPublicToken = 'pk.eyJ1Ijoibmt0YiIsImEiOiJjazhscjEwanEwZmYyM25xbzVreWMyYTU1In0.dcztuEUgjlhgaalrc_KLMw'
mbSecretToken = 'sk.eyJ1Ijoibmt0YiIsImEiOiJja2kyN3c5bDY2eWZxMnNsNjFvcDQzbHNiIn0.Xld2-RbDSScvhfIfdAqwTA'
userName = 'nktb'

# Очередь для загрузки тайлсетов
tiles_queue = []

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

# Функция получения изохронов через API MapBox
def get_iso(profile, lon, lat, times):

	urlBase = 'https://api.mapbox.com/isochrone/v1/mapbox'
	query = urlBase + '/' + profile + '/' + lon + ',' + lat + '?contours_minutes='+times+'&polygons=true&access_token=' + mbPublicToken
	headers = {'Content-type': 'Application/json', 'Accept': 'application/json'}
	data = ''
	result = send_request("get", query, data, headers)
	
	return result

# Функция построения маршрута
def get_route(first_point,last_point):
	urlBase = 'https://api.mapbox.com/directions/v5/mapbox/driving/'
	query = urlBase +first_point+";"+last_point+'?geometries=geojson'+'&access_token=' + mbPublicToken
	headers = {'Content-type': 'Application/json', 'Accept': 'application/json'}
	result = send_request("get", query, '', headers)

	return result

# Геокодирование адреса
def get_location(address):
	urlBase = 'https://api.mapbox.com/geocoding/v5/mapbox.places/'
	query = urlBase +address+'.json?limit=1&access_token=' + mbPublicToken
	headers = {'Content-type': 'Application/json', 'Accept': 'application/json'}
	result = send_request("get", query, '', headers)

	return result

# Запуск внешних команд	
def run_cmd(cmd):
	os.system(cmd+" --token "+mbSecretToken+" > result")
	message = open('result','r').read()
	os.system("rm result")
	print(message)
	return message

# Загружаем на сервер source для tileset
def upload_tile_source(city_code, layer_name, source_url):
	cmd = "tilesets upload-source "+userName+" "+city_code+"-"+layer_name+" "+source_url+" --replace"
	os.system(cmd+" --token "+mbSecretToken+" > result") 
	res_str = open('result','r').read()
	if res_str != "":
		result = json.loads(res_str)
		return result['id']
	else:
		return res_str

# Записываем рецепт в файл
def write_tile_recipe(source_id, layer_name):
	recip = {
		"version": 1,
		"layers": {
			layer_name: {
				"source": source_id,
				"minzoom": 0,
				"maxzoom": 13
			}
		}
	}
	file_out = open("recipe.json","w")
	file_out.write(json.dumps(recip))
	file_out.close()

# Удаление файла с рецептом
def remove_recipe():
	os.system("rm recipe.json") 

# Проверка статуса очереди
def check_queue_status():
	print("Checking for tilests queue status:")
	for tile_id in tiles_queue:
		cmd = "tilesets status "+tile_id
		status = json.loads(run_cmd(cmd))['status']
		
		if status == 'success':
			tiles_queue.remove(tile_id)

	if len(tiles_queue) == 0:
		return 'OK'
	else:
		print("No available queue for tileset. Waiting 60 sec...")
		return 'WAIT'


# Создаём tileset
def create_tileset(city_code, layer_name):
	# Ждём, когда освободится очередь на загрузку
	while True:
		if check_queue_status() == 'OK':
			break
		else:
			time.sleep(60)

	# Создаём tileset
	tile_id = userName+"."+city_code+"-"+layer_name
	create_cmd = "tilesets create "+tile_id+" --recipe recipe.json --name "+city_code+"-"+layer_name
	result = run_cmd(create_cmd)

	# Добавляем в очередь новый слой
	tiles_queue.append(tile_id)

	# Удаляем tileset, если такой уже есть
	if "already exists" in json.loads(result)["message"]:
		del_cmd = "tilesets delete "+tile_id+" -f"
		run_cmd(del_cmd)
		run_cmd(create_cmd)

	# Публикуем tileset
	pub_cmd = "tilesets publish "+tile_id
	run_cmd(pub_cmd)

	# Загружаем связь города с вектором в БД
	p = postgres.Postgres()
	p.upload_layer(layer_name)

	# Удаляем файл рецепта
	#remove_recipe()

	return tile_id

# Создание вектора для слоя с покрытием остановок
def create_stops_cover_tileset(city_code):
	# Загружаем geojson файл с изохроном на сервер mapbox. Создаём Source
	source_id = upload_tile_source(city_code,"stops_cover", "../out/stops_cover/stops_cover.geojson")
	# Создаём рецепт
	write_tile_recipe(source_id, "stops_cover")
	# Создаём и публикуем TileSet
	create_tileset(city_code, "stops_cover")

# Создание вектора для остановок
def create_stations_tileset(city_code):
	# Загружаем geojson файл с изохроном на сервер mapbox. Создаём Source
	source_id = upload_tile_source(city_code,"bus_stops", "../out/stations/geojson/stations.geojson")
	# Создаём рецепт
	write_tile_recipe(source_id, "bus_stops")
	# Создаём и публикуем TileSet
	create_tileset(city_code, "bus_stops")

# Создание вектора для маршрутов
def create_routes_tileset(city_code):
	# Загружаем geojson файл с изохроном на сервер mapbox. Создаём Source
	source_id = upload_tile_source(city_code,"routes", "../out/routes/geojson/routes.geojson")
	# Создаём рецепт
	write_tile_recipe(source_id, "routes")
	# Создаём и публикуем TileSet
	create_tileset(city_code, "routes")

# Создание вектора для плотности маршрутов
def create_routes_density_tileset(city_code):
	# Загружаем geojson файл с изохроном на сервер mapbox. Создаём Source
	source_id = upload_tile_source(city_code,"density", "../out/density/routes_density.geojson")
	# Создаём рецепт
	write_tile_recipe(source_id, "density")
	# Создаём и публикуем TileSet
	create_tileset(city_code, "density")

# Создание вектора для слоя с расстояниями между остановками
def create_stops_distance_tileset(city_code):
	# Загружаем geojson файл с изохроном на сервер mapbox. Создаём Source
	source_id = upload_tile_source(city_code,"stops_distance", "../out/stops_distance/stops_distance.geojson")
	# Создаём рецепт
	write_tile_recipe(source_id, "stops_distance")
	# Создаём и публикуем TileSet
	create_tileset(city_code, "stops_distance")


# Создание вектора для слоя с домами далеко от остановок
def create_houses_far_stops_tileset(city_code):
	# Загружаем geojson файл с изохроном на сервер mapbox. Создаём Source
	source_id = upload_tile_source(city_code,"houses", "../out/houses_far_stops/houses_far_stops.geojson")
	# Создаём рецепт
	write_tile_recipe(source_id, "houses_far_stops")
	# Создаём и публикуем TileSet
	create_tileset(city_code, "houses_far_stops")

# Создание вектора со слоем для ДТП
def create_dtp_map_tileset(city_code):
	# Загружаем geojson файл с изохроном на сервер mapbox. Создаём Source
	source_id = upload_tile_source(city_code,"dtp_map", "../out/dtp_map/dtp_map.geojson")
	# Создаём рецепт
	write_tile_recipe(source_id, "dtp_map")
	# Создаём и публикуем TileSet
	create_tileset(city_code, "dtp_map")

# Запуск Создания вектора
def run_create_tileset(city_code,layer_name):
	# Ждём, когда освободится очередь на загрузку
	while True:
		if check_queue_status() == 'OK':
			break
		else:
			time.sleep(60)
	# Загружаем geojson файл с изохроном на сервер mapbox. Создаём Source
	source_id = upload_tile_source(city_code,"dtp_map", "../out/"+layer_name+"/"+layer_name+".geojson")
	# Создаём рецепт
	write_tile_recipe(source_id, layer_name)
	# Создаём и публикуем TileSet
	create_tileset(city_code, layer_name)

if __name__ == '__main__':
	print("run MapBox")
	#print(run_cmd('tilesets status nktb.TUL-traffic_cameras'))

	#run_create_tileset("PRM","dtp_ochagi")

	#run_create_tileset("STV","traffic_cameras")

	#run_create_tileset("NNG","dtp_map")
	#create_stations_tileset('TUL')

	create_houses_far_stops_tileset('OMS')