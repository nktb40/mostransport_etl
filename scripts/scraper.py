import os
import time
import json
from selenium import webdriver
#from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.proxy import Proxy, ProxyType
import postgres
import shapely
from shapely.geometry import *
import re
import glob

# Список прокси адресов
proxy_list = []

# Инициируем WebDriver
def create_driver(proxy_flag):
	options = Options()
	options.headless = True
	options.add_argument("--window-size=1920,1200")

	if proxy_flag == True:
		proxy = get_proxy()
		options.add_argument('--proxy-server='+proxy['addr'])
		proxy['state']='used'

	DRIVER_PATH = '/usr/bin/chromedriver'

	driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)
	
	return driver

def get_proxy():
	global proxy_list

	if len(proxy_list) == 0:
		proxy_list = download_proxy_list()

	proxy = list(filter(lambda x: x['state'] == 'new', proxy_list))[0]
	return proxy

# Функция получения списка proxy адресов
def download_proxy_list():
	driver = create_driver(False)
	url = 'https://www.proxy-list.download/HTTP'
	proxy_list = []

	try:
		read_page(driver, url)

		# Выбираем страну
		driver.find_element_by_css_selector('#country-select dt a').click()
		driver.find_element_by_css_selector("#mislista li input[name='Russia']").click()
		driver.find_element_by_css_selector("#miboto").click()

		rows = driver.find_elements_by_css_selector('#tabli tr')

		for row in rows:
			r = row.find_elements_by_css_selector('td')
			ip = r[0].get_attribute('innerText')
			port = r[1].get_attribute('innerText')
			proxy_list.append({'state':'new', 'addr': 'http://'+ip+':'+port})
		print(proxy_list)
		return proxy_list

	finally:
		# Закрываем браузер
		driver.quit()

# Чтение страницы по заданному URL
def read_page(driver, url):
	driver.get(url)
	return driver

# Скачивание архива со списком домов
def download_houses_file(reg_name):
	print("Start download_houses_file",reg_name)

	# Удаляем старые архивы
	os.system("rm export-reestrmkd*.zip")

	# Инициализируем браузер
	driver = create_driver(False)

	url = 'https://www.reformagkh.ru/opendata?geo=reset&cids=house_management'
	read_page(driver, url)
	
	# Отрываем панель Регион
	driver.find_element_by_css_selector('a[href="#panel2"]').click()
	
	# Открываем список регионов
	driver.find_element_by_css_selector('#panel2 .dropdown button[data-id="appeal_category"]').click()
	
	# Выбираем необходимый регион
	regions = driver.find_elements_by_css_selector('#panel2 .dropdown .dropdown-menu .inner ul li a')
	next(x for x in regions if x.find_element_by_class_name('text').text.lower().startswith(reg_name.lower())).click()
	
	# Скачиваем архив
	form_results = driver.find_element_by_css_selector('#opendata-form section .container')
	p = next(x for x in form_results.find_elements_by_tag_name('p') if x.text.startswith('Реестр домов'))
	row = form_results.find_elements_by_class_name('row')[form_results.find_elements_by_tag_name('p').index(p)+1]
	row.find_element_by_xpath("//a[.='Экспорт']").click()

	# Ждём скачивания файла
	while len(glob.glob("export-reestrmkd*.zip")) == 0:
		time.sleep(5)

	# Закрываем браузер
	driver.quit()
	print("Finish download_houses_file",reg_name)

# Функция скачивания файла ДТП
def download_dtp_file(region_name):
	print("Start download_dtp_file",region_name)
	# Инициализируем браузер
	driver = create_driver(False)

	url = 'https://beta.dtp-stat.ru/opendata/'
	read_page(driver, url)

	# Выбираем необходимый регион
	regions = driver.find_elements_by_css_selector('table tbody tr')
	row = next(x for x in regions if region_name.lower() in x.find_element_by_css_selector('td b').text.lower())

	#Скачиваем файл
	link = row.find_elements_by_tag_name('td')[3].find_element_by_tag_name('a')
	link.click()

	# Ждём скачивания файла
	file_name = link.get_attribute('href').split('/')[-1]

	while os.path.exists(file_name) == False:
		time.sleep(5)

	# Закрываем браузер
	driver.quit()

	# Переименовываем файл и переносим в out
	os.system("mkdir ../out/dtp_map")
	os.system("mv "+file_name+" ../out/dtp_map/dtp_map_src.geojson")

	print("Finish download_dtp_file",region_name)


# Функция загрузки очагов ДТП
def download_dtp_ochagi(city_code, region_name):
	print("Start download_dtp_ochagi",region_name)

	# Получаем координаты города
	city = postgres.send_query("select name, bbox from cities where code='"+city_code+"'")
	city_name = city[0]
	bbox = city[1]
	box = shapely.geometry.box(bbox[0], bbox[1], bbox[2], bbox[3], ccw=True)

	# Инициализируем браузер
	driver = create_driver(False)

	try:

		url = 'http://stat.gibdd.ru/'
		read_page(driver, url)

		# Отрываем Места концентрации ДТП
		driver.find_element_by_css_selector('#placesAction').click()

		# Открываем список регионов
		driver.find_element_by_css_selector('#place_region .dui-dropdown__btn_drop').click()
		# Выбираем необходимый регион
		regions = driver.find_elements_by_css_selector('#place_region .dui-dropdown .dui-dropdown__body ul li')
		row = next(x for x in regions if region_name.lower() in x.find_element_by_tag_name('span').get_attribute("innerHTML").lower())
		reg_id = row.get_attribute("id")
		row.find_element_by_tag_name('span').click()

		features = []

		# В цикле выбираем период и скачиваем файлы
		for p in ['2019','2020']:
			# Открываем список периодов
			driver.find_element_by_css_selector('#place_date .dui-dropdown__btn_drop').click()
			# Выбираем период
			periods = driver.find_elements_by_css_selector('#place_date .dui-dropdown .dui-dropdown__body ul li')

			row = next(x for x in periods if x.find_element_by_css_selector('span.dui-nested-list__text').get_attribute("innerHTML") == p)
			row.click()
			# Показать карту
			driver.find_element_by_css_selector('#place_apply').click()

			# Отправляем запрос на получение списка полигонов с точками
			params = {
						"cpType":0,
						"date":"YEAR:"+p,
						"regId":reg_id
					}
			cps = gibdd_post(driver, "getCPoints", params)['cps']

			for cp in cps:
				geometry = shape(json.loads(cp['geoData']))
				if geometry.intersects(box) == True:
					b = geometry.bounds

					# Отправляем запрос на получение списка точек
					params = {
								"date":"YEAR:"+p,
								"regId":reg_id,
								"cpType":0,
								"lats":[b[3],b[1]],
								"lngs":[b[0],b[2]]
							}
					places = gibdd_post(driver, "getDTPs", params)
					#print(places)

					for pl in places:
						params = {"emPlaceId":pl['emPlaceId']}
						d = gibdd_post(driver, "getDTPFullData", params)
						
						f = {
							'type': 'Feature',
							'properties':
								{
									'place_id': pl['emPlaceId'],
									'dtp_num': d['emtpNum'],
									'address': d['road']+d['street'],
									'category':d['emTypeName'],
									'region':city_name,
									'parent_region':region_name,
									'datetime': d['emDate'] +' '+ d['emTime'],
									'injured_count': d['hurtCount'],
									'dead_count': d['lossCount'],
									'injured_child_count': d['hurtChildCount'],
									'dead_child_count': d['lossChildCount']
								}
							}
						f['geometry'] = {"type": "Point", "coordinates": [pl['lng'],pl['lat']]}	
						features.append(f)	

		# Сохраняем данные в файл
		collection = {'type': 'FeatureCollection', 'features':features}
		os.system('mkdir ../out/dtp_ochagi')
		with open('../out/dtp_ochagi/dtp_ochagi.geojson', 'w') as file_out:
			file_out.write(json.dumps(collection,ensure_ascii=False))	
		
	finally:
		# Закрываем браузер
		driver.quit()

	print("Finish download_dtp_ochagi",region_name)

# Функция отправки JS запросов на странице gibdd
def gibdd_post(driver, fun_name, params):
	time.sleep(1)
	js = ('var result; '+
		  '$.ajax({url:"http://stat.gibdd.ru/places/'+fun_name+'",type:"POST",'+
		  '  data:JSON.stringify('+json.dumps(params)+'),'+
		  '  contentType:"application/json; charset=utf-8",'+
		  '  dataType:"json",'+
		  '  success: function(data){result = data}'+
		  '}); '+
		  'const delay = millis => new Promise((resolve, reject) => {setTimeout(_ => resolve(), millis)});'+
		  'while(result == undefined){await delay(100);} '+
		  'return result;')
	#print(js)
	return driver.execute_script(js)



# функция загрузки дорожных камер 
def download_traffic_cameras(city_code, region_name):
	print("Start download_traffic_cameras",region_name)

	# Инициализируем браузер
	driver = create_driver(False)
	url = 'https://xn--90adear.xn--p1ai/milestones'
	try:
		read_page(driver, url)

		# Выбираем необходимый регион
		regions = driver.find_element_by_name('region_code').find_elements_by_tag_name('option')
		row = next(x for x in regions if region_name.lower() in x.text.lower())
		row.click()

		# Находим скрипт с списком камер
		script_txt = driver.find_element_by_css_selector('#list script').get_attribute('innerHTML')
		script_txt = script_txt[script_txt.index('data.points.push('):script_txt.index('milestonesMap.setData(data);')]
		# Делим на строки
		script_lines = script_txt.replace('\n','').split('data.points.push(')

		# Выделяем объекты с описанием камер из скрипта
		features = []
		for s in script_lines[1:]:
			#print(s)
			# Делаем замены в строках, чтобы корректно преобразовать в json
			s = re.sub(' +', ' ', s)
			s = re.sub('\t+', ' ', s)
			s = s.rstrip().rstrip(');')
			s = s.replace(', }','}')
			s = s.replace("\\","/").replace('/"','\\"')
			#s = s.replace('href=\\"',"href='").replace('/\\">',"/'>")
			#s = s.replace("\"yandexMapList\"","'yandexMapList'")
			s = s.replace('properties','"properties"')
			s = s.replace('balloonContentBody','"balloonContentBody"')
			s = s.replace('hintContent','"hintContent"')
			s = s.replace('balloonContentHeader','"balloonContentHeader"')
			s = s.replace('geometry','"geometry"')
			s = s.replace('type','"type"')
			s = s.replace('coordinates','"coordinates"')

			# Парсим json
			cam = json.loads(s)
			# Создаём фичу 
			c = cam['properties']['balloonContentBody']
			f = {
				'type': 'Feature',
				'properties':
					{
						'address': cam['properties']['hintContent'],
						'functions': str(c[c.index('<li>'):c.index('</ul>')].replace('<li>','').rstrip('</li>').split('</li>') if 'ul' in c else [])
					},
				'geometry': cam['geometry']
				}

			crd = cam['geometry']['coordinates']
			f['geometry'] = {"type": "Point", "coordinates": [crd[1],crd[0]]}
			features.append(f)

		# Сохраняем данные в файл
		collection = {'type': 'FeatureCollection', 'features':features}
		os.system('mkdir ../out/traffic_cameras')
		with open('../out/traffic_cameras/traffic_cameras.geojson', 'w') as file_out:
			file_out.write(json.dumps(collection,ensure_ascii=False))	

	finally:
		# Закрываем браузер
		driver.quit()

	print("Finish download_traffic_cameras",region_name)

# Функция геокодирования адреса через яндекс карты
def get_yndx_location(address):
	# Инициализируем браузер
	driver = create_driver(False)
	url = 'https://yandex.ru/maps/'
	try:
		read_page(driver, url)

		# Сохраняем текущий URL
		cur_url = driver.current_url

		# Ищем строку поиска и вводим адрес
		search_input = driver.find_element_by_css_selector('input.input__control[type="search"]')
		search_input.send_keys(address + Keys.ENTER)
		# Ждём выполнения запроса
		WebDriverWait(driver, 5).until(EC.url_changes(cur_url))
		#WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME,"toponym-card-title-view__coords-badge")))
		time.sleep(2)
		# Проверяем есть ли на странице элемент с координатами и находим координаты

		elements = driver.find_elements_by_css_selector('.toponym-card-title-view__coords-badge')
		
		if len(elements) > 0:
			coords = elements[0].get_attribute("innerText").split(", ")
			result = [{"geometry": { "type": "Point", "coordinates": [float(coords[1]), float(coords[0])]}}]
		else:
			new_url = driver.current_url
			if new_url.index('/?ll=') >=0:
				coords_str = new_url[new_url.index('/?ll=')+5:new_url.index('&')]
				coords = coords_str.split('%2C')
				result = [{"geometry": { "type": "Point", "coordinates": [float(coords[1]), float(coords[0])]}}]
			else:
				result = []
		
		# Добавить очистку поля для ввода
		return result

	except Exception as e:
		raise e
	finally:
		# Закрываем браузер
		driver.quit()


# Функция геокодирования адреса через яндекс карты
def get_yndx_location_light(driver,address):
	try:
		# Сохраняем текущий URL
		cur_url = driver.current_url

		# Ищем строку поиска и вводим адрес
		# while len(driver.find_elements_by_css_selector('input.input__control[type="search"]')) == 0:
		# 	driver.quit()
		# 	driver = create_driver(True)
		# 	url = 'https://yandex.ru/maps/'
		# 	read_page(driver, url)

		search_input = driver.find_element_by_css_selector('input.input__control[type="search"]')
		search_input.send_keys(address + Keys.ENTER)

		# Ждём выполнения запроса
		WebDriverWait(driver, 5).until(EC.url_changes(cur_url))
		#WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME,"toponym-card-title-view__coords-badge")))
		time.sleep(1)

		# Проверяем есть ли на странице элемент с координатами и находим координаты
		elements = driver.find_elements_by_css_selector('.toponym-card-title-view__coords-badge')
		
		if len(elements) > 0:
			coords = elements[0].get_attribute("innerText").split(", ")
			result = [{"geometry": { "type": "Point", "coordinates": [float(coords[1]), float(coords[0])]}}]
		else:
			new_url = driver.current_url
			if new_url.index('/?ll=') >=0:
				coords_str = new_url[new_url.index('/?ll=')+5:new_url.index('&')]
				coords = coords_str.split('%2C')
				result = [{"geometry": { "type": "Point", "coordinates": [float(coords[1]), float(coords[0])]}}]
			else:
				result = []
		
		# Очищаем поле для ввода
		close_btn = driver.find_elements_by_css_selector('form.search-form-view .small-search-form-view__button')[-1]
		close_btn.click()

		# Возвращаем результат
		return result

	except Exception as e:
		print("ERROR YANDEX",address)
		raise e
		# Закрываем браузер
		driver.quit()

# Тестовая функция для отображения списка запросов
def get_yndx_requests():
	# Инициализируем браузер
	driver = create_driver(False)
	url = 'https://yandex.ru/maps/'
	try:
		read_page(driver, url)

		for request in driver.requests:
			if request.response:
				print(
					#request.path,
					#request.querystring,
					#request.params,
					#request.response.status_code,
					#request.response.headers['Content-Type']
					request.response.body
				)
	finally:
		# Закрываем браузер
		driver.quit()


def check_ip():
	# Инициализируем браузер
	driver = create_driver(True)

	url = 'https://www.whatismyip.com/'
	try:
		read_page(driver, url)
		time.sleep(10)
	finally:
		# Закрываем браузер
		driver.quit()

# ===============================================
if __name__ == '__main__':
	print("Scrapper Begin")

	#download_dtp_file("Тульская область")

	#download_dtp_ochagi("TUL","Тульская область")

	#download_traffic_cameras("KAZ","Республика Татарстан")

	#download_houses_file("Ивановская область")

	#get_yndx_location("край. Ставропольский, г. Ставрополь, ул. Социалистическая, д. 6/1")

	#print(get_yndx_location("Россия, Великий Новгород, Александра Корсунова пр-кт, 40-2"))

	print(get_yndx_location("Россия, Великий Новгород, Мира пр-кт, 2"))