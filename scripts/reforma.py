import os
from csv import DictReader
import glob
import osm
import json
import math
import scraper
from shapely.geometry import *
from functools import reduce

# Функция достаёт список домов из скаченного файла, выполняет геокодирование адреса и преобразует в geojson
def get_city_houses(city_code, city_name, region_name):
	# Скачиваем файл с домами
	scraper.download_houses_file(region_name)

	# Определяем название архива
	arch_name = glob.glob("export-reestrmkd*.zip")[0]
	# Разархивируем архив
	os.system("unzip "+arch_name)
	# Читаем csv файл и выполняем фильтрацию по городу
	csv_name = arch_name.replace(".zip",".csv")
	houses_src = []
	houses = []

	# Считываем список домов в массив
	with open(csv_name, 'r') as read_obj:
		reader = DictReader(read_obj, delimiter=';')
		for row in reader:
			houses_src.append(row)

	# Считаваем из параметров кол-во жителей
	with open("../in/params/params.json", 'r') as file:
		city_params = json.load(file)['population']

	# Расчитываем жилую площадь на одного жителя
	area_per_pers = reduce((lambda x, y: x+y), map(lambda x: x['area_residential'], houses_src))

	area_residential = reduce((lambda x, y: x+y), map(lambda x: float(x['area_residential'].replace(',','.')) if ['area_residential'] != '' else 0, houses_src))
	area_per_pers = round(area_residential/city_params['population'],2)

	# Проходимся по списку домов и выполняем геокодирование
	for row in houses_src:
		if (row['formalname_city'] == city_name) or (row['formalname_region'] == city_name and row['formalname_city'] == ''):

			# Очистка названия улицы 
			if row['shortname_street'] == 'проезд' and '-й' in row['formalname_street']:
				street_name = row['formalname_street'].replace('-й','-й проезд')
			elif row['shortname_street'] == 'ул' and row['formalname_street'].endswith('ая'):
				street_name = row['formalname_street']+' '+row['shortname_street']
			elif row['shortname_street'] == 'пер':
				if row['formalname_street'].endswith('ий') or row['formalname_street'].endswith('ой'):
					street_name = row['formalname_street']+' переулок'
				else:
					street_name = 'переулок '+row['formalname_street']
			elif row['shortname_street'] == 'наб':
				if row['formalname_street'].endswith('ая'):
					street_name = row['formalname_street']+' набережная'
				else:
					street_name = 'набережная '+row['formalname_street']
			elif row['shortname_street'] == 'пр-кт':
				if row['formalname_street'].endswith('ий'):
					street_name = row['formalname_street']+' проспект'
				else:
					street_name = 'проспект '+row['formalname_street']	
			elif row['shortname_street'] == 'мкр':
				street_name = 'микрорайон '+row['formalname_street']	
			else:
				street_name = row['shortname_street']+' '+row['formalname_street']

			# Очистка номера дома
			if 'дом' in row['house_number']:
				row['house_number'] = row['house_number'].replace('дом','').strip()

			# Расчёт кол-ва жителей
			if row['area_residential'] != '':
				population = math.ceil(float(row['area_residential'].replace(',', '.'))/area_per_pers)
			else:
				population = 0

			# Формирование json объекта с параметрами дома
			house = {
				'type': 'Feature',
				'properties':
					{
						'source_id': row['\ufeffid'],
						'country_code': 'RU',
						'city_code': city_code,
						'city_name': row['formalname_city'],
						'street_name': street_name,
						'house_number': row['house_number'],
						'building': row['building'],
						'block': row['block'],
						'letter': row['letter'],
						'address': row['address'],
						'floor_count_min': row['floor_count_min'],
						'floor_count_max': row['floor_count_max'],
						'entrance_count': row['entrance_count'],
						'area_total': row['area_total'],
						'area_residential': row['area_residential'],
						'population': population
					}
				}

			houses.append(house)
	#features = osm.get_houses_location(houses)
	features = osm.geocode_addresses_in_threads(city_code,houses,5)
	#print(len(features))
	collection = {'type': 'FeatureCollection', 'features':features}
	
	os.system('mkdir ../out/houses')
	with open('../out/houses/houses.geojson', 'w') as file_out:
		file_out.write(json.dumps(collection,ensure_ascii=False))

	# Удаляем временные файлы
	os.system("rm "+arch_name)
	os.system("rm "+csv_name)

# Получение списка домов вне изохрона остановок
def get_houses_far_from_stops():
	print("Start get_houses_far_from_stops")
	houses_out = []
	# Читаем файл с изохроном покрытия остановок
	with open('../out/stops_cover/stops_cover.geojson', 'r') as c_file:
		stops_cover = json.load(c_file)['features'][0]
		cover_geometry = shape(stops_cover['geometry'])

		# Читаем файл с домами
		with open('../out/houses/houses.geojson', 'r') as h_file:
			houses = json.load(h_file)['features']
			for house in houses:
				house_geometry = house['geometry']
				# Если есть координаты, проверяем находится ли дом внутри покрытия остановкми 
				if house_geometry['coordinates'] != []:
					if shape(house_geometry).within(cover_geometry) == False:
						houses_out.append(house)

	collection = {'type': 'FeatureCollection', 'features':houses_out}
	
	os.system('mkdir ../out/houses_far_stops')
	with open('../out/houses_far_stops/houses_far_stops.geojson', 'w') as file_out:
		file_out.write(json.dumps(collection,ensure_ascii=False))

	print("Finish get_houses_far_from_stops")

# ===============================================
if __name__ == '__main__':
	print("Reforma Begin")
	# Скачиваем файл с реформы жкх
	#download_houses_file('Тульская область')

	# Конвертируем файл в geoJson и добавляем координаты домов
	#get_city_houses("TUL","Тула")

	#get_city_houses('PSK', 'Псков', 'Псковская область')
	#get_houses_far_from_stops()