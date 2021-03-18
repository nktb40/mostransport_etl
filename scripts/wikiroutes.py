from common import *
import time
import json

TRANSPORT_TYPES_MAPPING = {
	'автобусы': TransportType.BUS,
	'трамваи': TransportType.TRAM,
	'маршрутки': TransportType.BUS,
	'троллейбусы': TransportType.TROLLEYBUS,
	'водный транспорт': TransportType.FERRY,
	'фуникулёры': TransportType.FUNICULAR,
	'электрички': TransportType.RAIL,
	'метро': TransportType.METRO
}

CLASS_CITY_SEARCH_PANEL = 'CitiesPanel-search'
XPATH_ROUTE_LIST_TYPE_BUTTON = "//span[text() = '{}']"
CSS_SUBLIST_BLOCK = 'div.typeBlock'
CSS_SUBLIST_HEADER = 'span.typeHeader-name'
XPATH_ACTIVE_ROUTE = ".//a[not(contains(@class,'no-active')) and not(contains(@class,'hidden'))]"

class RoutesListType(Enum):
	ALL = 'Все маршруты'
	CITY = 'Городские'
	INTER_CITY = 'Пригород/Межгород'

# Функция получения списка маршрутов города с wikiroutes
def get_routes(city, list_type = RoutesListType.ALL):
	# Инициализируем браузер
	driver = create_driver(False)

	url = 'https://wikiroutes.info/msk/catalog'
	try:
		driver.get(url)
		
		# Открываем панель с выбором города
		driver.find_element_by_id('city').click()
		# Вбиваем название города в поиск
		city_input = find_interactive_element(driver, By.CLASS_NAME, CLASS_CITY_SEARCH_PANEL)
		city_input.send_keys(city)
		# Выбираем город
		driver.find_elements_by_css_selector('div.CitiesPanel-cityLabel')[0].click()

		if list_type != RoutesListType.ALL:
			list_type_btn = find_interactive_element(driver, By.XPATH, XPATH_ROUTE_LIST_TYPE_BUTTON.format(list_type.value))
			list_type_btn.click()

		routes = []

		sublist_blocks = driver.find_elements_by_css_selector(CSS_SUBLIST_BLOCK)
		for sublist_block in sublist_blocks:
			header = sublist_block.find_element_by_css_selector(CSS_SUBLIST_HEADER)
			type_text = header.get_attribute('innerHTML').lower()
			try:
				transport_type = TRANSPORT_TYPES_MAPPING[type_text]
			except:
				print('Transport type {} is unknown!'.format(type_text))

			route_elements = sublist_block.find_elements(By.XPATH, XPATH_ACTIVE_ROUTE)
			for route_element in route_elements:
				route = {
					'transport_type': transport_type,
					'title': route_element.get_attribute('title')
				}
				routes.append(route)

		return routes
	except Exception as e:
		print(str(e))
	finally:
		driver.quit()
		pass



if __name__ == "__main__":
	import argparse
	import sys

	if len(sys.argv) > 1:
		parser = argparse.ArgumentParser(description='Get routes list from wikiroutes.')
		parser.add_argument('city', type=str, help='City')
		parser.add_argument('output', type=str, help='Output filename')
		args = parser.parse_args()

		city = args.city
		output = args.output
	else:
		city = input('City: ')
		output = input('Output: ')
	
	routes = get_routes(city, RoutesListType.CITY)

	with open(output, 'w') as f:
		f.write(json.dumps(routes))
