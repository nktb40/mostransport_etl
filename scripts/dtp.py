import os
import glob
import pandas as pd
import scraper
import json
from datetime import datetime
from csv import DictReader
import osm


# Выгрузка очагов ДТП
def download_dtp_ochagi(city_code,region_name):
	# Конвертируем исходные файлы в CSV
	scraper.download_dtp_ochagi(city_code,region_name)

def download_dtp(city_code,region_name):
	scraper.download_dtp_file(region_name)
	transform_dtp(city_code)

# Функция конвертирования файла с очагами ДТП из XLS в CSV
def convert_to_csv():
	frames = []

	# Разархивируем
	for arch in glob.glob("../out/dtp_ochagi/*.zip"):
		year = arch.split("/")[-1].split('.')[0]
		os.system("unzip "+arch+" -d ../out/dtp_ochagi")

		# Переносим в DF
		file = glob.glob("../out/dtp_ochagi/*.xls")[0]
		df = pd.read_excel (file)
		df['year'] = year
		frames.append(df)
		os.system("rm ../out/dtp_ochagi/*.xls")

	# Объединяем DF
	df = pd.concat(frames)

	# Выгружаем в CSV
	df.to_csv ('../out/dtp_ochagi/ochagi.csv', index = None, header=True)


# В исходных данных ДТП трансформируем списки [] в строки, чтобы их не игнорировал tilesets
def transform_dtp(city_code):

	# Трансформируем исходные данные для векторного файла (преобразуем списки в строковый тип)
	with open('../out/dtp_map/dtp_map_src.geojson', 'r') as read_obj:
		dtps = json.load(read_obj)['features']

	# Список трансформированных фич
	features = []

	# Проходимся по списку фич и преобразовывем списки в строки
	for d in dtps:
		p = d['properties']

		f = {
			'type': 'Feature',
			'geometry': d['geometry'],
			'properties':
			{
				'id': p['id'],
				'datetime': p['datetime'],
				'light': p['light'],
				'region': p['region'],
				'parent_region': p['parent_region'],
				'address': p['address'],
				'weather': ', '.join(p['weather']),
				'category': p['category'],
				'severity': p['severity'],
				'dead_count': p['dead_count'],
				'injured_count': p['injured_count'],
				'participants_count': p['participants_count'],
				'road_conditions': ', '.join(p['road_conditions']),
				'participant_categories': ', '.join(p['participant_categories'])
			}
		}

		features.append(f)

	# Добавляем  дтп без пострадавших, если есть такой файл
	if len(glob.glob('../out/dtp_map/dtp_no_injured.geojson')) == 1:
		with open('../out/dtp_map/dtp_no_injured.geojson', 'r') as read_obj:
			dtps = json.load(read_obj)['features']
			features.extend(dtps)
	elif len(glob.glob('../out/dtp_map/dtp_not_injured_src.geojson')) == 1:
		features.extend(transform_no_injured())
	elif len(glob.glob('../out/dtp_map/dtp_not_injured.csv')) == 1:
		features.extend(geocode_dtp_not_injured(city_code))

	# Формироуем коллекцию
	collection = {'type': 'FeatureCollection', 'features':features}

	# Записываем данные в файл
	os.system('mkdir ../out/dtp_map')
	with open('../out/dtp_map/dtp_map.geojson', 'w') as file_out:
		file_out.write(json.dumps(collection,ensure_ascii=False))


# Трансформирование полей для данных о дтп без пострадавших в общий формат фала с ДТП
def transform_no_injured():
	with open('../out/dtp_map/dtp_no_injured_src.geojson', 'r') as read_obj:
		dtps = json.load(read_obj)['features']

	# Список трансформированных фич
	features = []

	# Проходимся по списку фич и преобразовывем списки в строки
	for d in dtps:
		p = d['properties']

		street = p['Улица'] if  p['Улица'] != None else ''
		house = p['Дом'] if  p['Дом'] != None else ''
		route = 'дорога ' + p['Дорога'] if  p['Дорога'] != None else ''
		km = str(p['Километр']) + ' км' if  p['Километр'] != None else ''
		m = str(p['Метр']) + ' м'  if  p['Метр'] != None else ''
		
		f = {
			'type': 'Feature',
			'geometry': d['geometry'],
			'properties':
			{
				'id': p['Номер'],
				'datetime': datetime.strptime(p['Дата']+' '+p['Время'], '%d.%m.%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S'),
				'light': 'Нет данных',
				'region': p['Место'].split(', ')[1] if len(p['Место'].split(', ')) > 1 else p['Место'].split(', ')[0],
				'parent_region': p['Место'].split(', ')[0],
				'address': (p['Место'].split(', ')[-1] + ', '+ street + ', '+ house + ' '+route+' '+km+' '+m).rstrip(),
				'weather': 'Нет данных',
				'category': p['Вид ДТП'],
				'severity': 'Нет пострадавших',
				'dead_count': p['Погибло'] + p['Погибло детей'],
				'injured_count': p['Ранено'] + p['Ранено детей'],
				'participants_count': 'Нет данных',
				'road_conditions': 'Нет данных',
				'participant_categories': 'Все участники'
			}
		}
		#print(f)
		features.append(f)

	return features

# Функция геокодирования адресов дтп из csv файла 
def geocode_dtp_not_injured(city_code):
	
	features = []

	with open('../out/dtp_map/dtp_not_injured.csv', 'r') as read_obj:
		dtps = DictReader(read_obj,delimiter=";")

		for d in dtps:
			# Проверяем, что есть пострадавшие
			if int(d['Погибло']) + int(d['Погибло детей']) + int(d['Погибло']) + int(d['Погибло детей']) == 0:
				street = d['Улица'] if  d['Улица'] != '' else ''
				house = d['Дом'] if  d['Дом'] != '' else ''
				route = 'дорога ' + d['Дорога'] if  d['Дорога'] != '' else ''
				km = str(d['Километр']) + ' км' if  d['Километр'] != '' else ''
				m = str(d['Метр']) + ' м'  if  d['Метр'] != '' else ''

				f = {
					'type': 'Feature',
					'properties':
					{
						'id': d['Номер'],
						'datetime': datetime.strptime(d['Дата']+' '+d['Время'], '%d.%m.%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S'),
						'light': 'Нет данных',
						'region': d['Место'].split(', ')[1] if len(d['Место'].split(', ')) > 1 else d['Место'].split(', ')[0],
						'parent_region': d['Место'].split(', ')[0],
						'address': (d['Место'].split(', ')[-1] + ', '+ street + ', '+ house + ' '+route+' '+km+' '+m).rstrip(),
						
						'house_number': house,
						'street_name': street,
						'city_name': d['Место'].split(', ')[-1],
						'country_code': 'RU',
						
						'weather': 'Нет данных',
						'category': d['Вид ДТП'],
						'severity': 'Нет пострадавших',
						'dead_count': d['Погибло'] + d['Погибло детей'],
						'injured_count': d['Ранено'] + d['Ранено детей'],
						'participants_count': 'Нет данных',
						'road_conditions': 'Нет данных',
						'participant_categories': 'Все участники'
					}
				}
				
				features.append(f)
	# Выполняем геокодирование
	features_with_geom = osm.geocode_addresses_in_threads(city_code,features,5)

	collection = {'type': 'FeatureCollection', 'features':features_with_geom}
	
	os.system('mkdir ../out/dtp_map')
	with open('../out/dtp_map/dtp_no_injured.geojson', 'w') as file_out:
		file_out.write(json.dumps(collection,ensure_ascii=False))
	
	return features_with_geom

if __name__ == '__main__':
	print("DTP Begin")

	#convert_to_csv()

	transform_dtp("VNO")

	#transform_no_injured()

	#geocode_dtp_not_injured("VNO")