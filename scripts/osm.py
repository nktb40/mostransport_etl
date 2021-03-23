# Модуль для геокодирования адресов через API нашего сервера Nominatim

from datetime import datetime
import numpy
import requests
import multiprocessing
import scraper
import postgres

def send_request(url, headers):
	#print("Sending request to:"+url)
	try:
		result = requests.get(url, headers=headers)
	except requests.exceptions.RequestException as e:  # This is the correct syntax
		print(e)
	return result.json()

# Функция формирования запроса на геокодирование адреса
def get_addr_geometry(house,driver,bbox):
	#urlBase = 'https://nominatim.openstreetmap.org/search?';
	try:
		urlBase = 'http://130.193.45.222/search?';
		query = (
			urlBase + 
			'street=' + house['house_number'] + ' ' + house['street_name'] +
			'&city=' + house['city_name'] +
			'&country='+house['country_code'] +
			'&format=geojson'+
			'&limit=1'+
			'&viewbox='+bbox+
			'&bounded=1'
			)
		# Если не указана улица, то запрос не выполняем
		if house['street_name'] == '':
			result = []
		else:
			headers = {'Content-type': 'Application/json', 'Accept': 'application/json'}
			result = send_request(query, headers)['features']
			# Если адрес не найден, то отправляем запрос в яндекс
			if len(result) == 0:
				address = 'Россия' + ', '+house['city_name']+', '+house['street_name']+', '+house['house_number']
				print("Try yandex search: ", address)
				result = scraper.get_yndx_location_light(driver,address)
				
		# Формирует результат работы функции
		if len(result) > 0:
			return {'status':'OK', 'data': result}
		else:
			return {'status':'ERROR', 'data':query}
	except Exception as e:
		raise e

# Функция геокодирования адресов домов
def geocode_addresses(houses, results, part, bbox):
	# Открываем страницу с яндекс картами для доп. геокодинга
	driver = scraper.create_driver(False)
	try:
		scraper.read_page(driver, 'https://yandex.ru/maps/')

		total_cnt = len(houses)
		i = 0
		err_cnt = 0

		for house in houses:
			i+=1
			print("Houses Location. Part ", part,", House ",i," from ",total_cnt)
			addr_params = house['properties']
			result = get_addr_geometry(addr_params,driver,bbox)

			if result['status'] == 'OK':
				house['geometry'] = result['data'][0]['geometry']
			else:
				print("ERROR: House not found",result['data'])
				house['geometry'] = {"type": "Point", "coordinates": []}
				err_cnt +=1

		print('Part',part,'errors count',err_cnt)

		results.extend(houses)
		
	except Exception as e:
		raise e
	finally:
		driver.quit()


# Запуск в многопоточном режиме
def geocode_addresses_in_threads(city_code,houses,threads_num):
	print("Start get houses location. Time:",datetime.now())
	p = postgres.Postgres()

	bbox_list = p.send_query("select bbox from cities where code='"+city_code+"'")[0]
	bbox = ','.join([str(i) for i in bbox_list])
	
	manager = multiprocessing.Manager()
	results = manager.list()
	chunks = numpy.array_split(houses,threads_num)
	processes = []

	for i in range(0,threads_num):
		print("Houses location. Part: "+str(i+1)+" len: "+str(len(chunks[i])))
		p = multiprocessing.Process(target=geocode_addresses, args=(chunks[i], results, str(i+1), bbox))
		processes.append(p)
		p.start()

	for process in processes:
		process.join()
		process.close()

	print("Finish get houses location. Time:",datetime.now())

	return list(results)
