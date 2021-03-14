from common import *
from browsermobproxy import Server
import json
from datetime import datetime
import os
import time

class Line:
	def __init__(self):
		self.stations = []
		self.geometry = {}
		self.boundaries = []

class Route:
	def __init__(self, line_id, name, type, seoname):
		self.line_id = line_id
		self.name = name
		self.type = type
		self.seoname = seoname
		self.recorded = False
		self.lines = []
		self.reversed = False
		self.intercity = False
		self.region = None

	def get_id(self):
		pattern = '%s_%s_%s_reversed' if self.reversed else '%s_%s_%s'
		return pattern  % (self.type, self.seoname, self.line_id)

	def get_local_id(self):
		return '%s_%s' % (self.type, self.seoname)


class Station:
	def __init__(self, id, name, coordinates):
		self.id = id
		self.name = name
		self.coordinates = coordinates
		self.routes = []
		self.visited = 0  # 0 - white, 1 - gray, 2 - black
		self.region = None

	
BASE_URL = 'https://maps.yandex.ru'
XPATH_SEARCH_INPUT = "//input[@type='search']"
XPATH_SEARCH_BUTTON = "//button[@type='submit']"

XPATH_STATION_BTN_QUERY = "//li[contains(@class,'masstransit-legend-group-view__item') and ./a[contains(@href, 'stops/%s')]]"
XPATH_ROUTE_BTN_QEURY = "//li[contains(@class,'masstransit-vehicle-snippet-view') and ./div[@class='masstransit-vehicle-snippet-view__row']/div[@class='masstransit-vehicle-snippet-view__info']/a[contains(@href, '%s')]]"
# thread = direction
XPATH_THREAD_BTN_QUERY = "//li[contains(@class,'masstransit-threads-view__item') and not(contains(@class,'_is-active'))]"

XPATH_SEARCH_CLEAR_BTN = "//button[span[div[contains(@class, '_type_close')]]]"
XPATH_STATION_SEARCH_ELEMENT = "//div[@class='search-snippet-view__body' and .//a[contains(text(),'Остановка общественного транспорта')]]"

STATION_REQUEST_TYPE = 'getStopInfo'
ROUTE_REQUEST_TYPE = 'getLine'

def fetch_routes(city_name, start_station_name):
	server = Server('browsermob-proxy')
	server.start()
	proxy = server.create_proxy()
	har_options = { 'captureContent': True }
	proxy.new_har('yandex', har_options)

	chrome_capabilities = webdriver.DesiredCapabilities.CHROME.copy()
	chrome_capabilities['acceptInsecureCerts'] = True
	proxy.add_to_capabilities(chrome_capabilities)

	driver = webdriver.Chrome(desired_capabilities=chrome_capabilities)

	driver.get(BASE_URL)

	scrapper = YandexMapsScrapper(driver, proxy, har_options)
	scrapper.fetch_routes(city_name, start_station_name)

	save_all(scrapper.stations, scrapper.routes)
	print('Done')
	server.stop()
	driver.quit()

def log_route_in(route):
	print('>> R' + route.get_local_id())

def log_route_out(route):
	print('<< R' + route.get_local_id())

def log_station_in(station):
	print('>>S ' + station.name)

def log_station_out(station):
	print('<<S ' + station.name)

def parse_response(request):
	return json.loads(request['response']['content']['text'])['data']

def create_station(station_info):
	return Station(station_info['id'], station_info['name'], station_info['coordinates'])

class YandexMapsScrapper:

	def __init__(self, driver, proxy, har_options):
		self.driver = driver
		self.proxy = proxy
		self.har_options = har_options
		self.routes = {}
		self.stations = {}


	def fetch_routes(self, city_name, start_station_name):
		search_input = self.driver.find_element(By.XPATH, XPATH_SEARCH_INPUT)

		search_input.send_keys(city)
		self.driver.find_element(By.XPATH, XPATH_SEARCH_BUTTON).click()

		WebDriverWait(self.driver, DEFAULT_INTERACTIVE_TIMEOUT) \
				.until(expected_conditions.presence_of_element_located((By.CLASS_NAME, 'card-title-view__title')))
		find_interactive_element(self.driver, By.XPATH, XPATH_SEARCH_CLEAR_BTN).click()

		search_input.send_keys('остановка ' + start_station_name)
		self.driver.find_element(By.XPATH, XPATH_SEARCH_BUTTON).click()
		find_interactive_element(self.driver, By.XPATH, XPATH_STATION_SEARCH_ELEMENT).click()

		WebDriverWait(self.driver, DEFAULT_INTERACTIVE_TIMEOUT) \
				.until(expected_conditions.presence_of_element_located((By.CLASS_NAME, 'card-title-view__title')))

		station_info = parse_response(self.pop_request(STATION_REQUEST_TYPE))
		station = create_station(station_info)
		self.parse_station(station, station_info)
		self.main_region = station.region
		self.stations[station.id] = station

		log_station_in(station)
		self.visit(station)

	def pop_request(self, request_type):
		requests = self.proxy.har['log']['entries']
		try:
			result = next(filter(lambda x: request_type in x['request']['url'], requests))
			self.proxy.new_har('yandex', self.har_options)  # clear har
			return result
		except:
			raise Exception('Request not found!')

	def parse_station(self, station, station_info):
		station.region = station_info['region']['id']
		for transport in station_info['transports']:
			route = Route(transport['lineId'], transport['name'], transport['type'], transport['seoname'])
			if route.get_id() not in self.routes:
				self.routes[route.get_id()] = route
			station.routes.append(route.get_id())
		return station

	def parse_route(self, route, route_info):
		new_stations = []
		lines = route_info['features']
		for line in lines:
			regions = line['MapsUIMetaData']['Region']
			# FIXME: use city_id???
			if regions[0]['id'] != regions[1]['id']:
				route.intercity = True
				break
			else:
				route.region = regions[0]['id']
			
			sections = []
			line_stations = []
			for feature in line['features']:
				if 'coordinates' in feature:  # Station
					station = create_station(feature)
					if station.id not in self.stations:
						self.stations[station.id] = station
						new_stations.append(station.id)
					line_stations.append(station.id)
				if 'points' in feature:  # sections
					points = list(feature['points'])
					if len(sections) > 0 and sections[-1] == points[0]:
						sections.pop()
					sections.extend(points)
			route_line = Line()
			route_line.stations = line_stations
			route_line.geometry = {
				'type': 'LineString',
				'coordinates': sections
			}
			route_line.boundaries = line['properties']['boundedBy']
			route.lines.append(route_line)
		return new_stations
	
	def visit(self, station):
		station.visited = 1
		for route_id in station.routes:
			route = self.routes[route_id]
			if not route.recorded and not route.intercity:

				if self.select_route(route):
					log_route_in(route)
					###
					try:
						route_info = parse_response(self.pop_request(ROUTE_REQUEST_TYPE))
						new_stations = self.parse_route(route, route_info)
						route.recorded = True

						for station_id in new_stations:
							new_station = self.stations[station_id]

							if self.select_station(new_station):
								log_station_in(new_station)
								####
								station_info = parse_response(self.pop_request(STATION_REQUEST_TYPE))
								self.parse_station(new_station, station_info)
								if new_station.region == self.main_region:
									self.visit(new_station)
								# else:  # remove intercity station
								#     stations.pop(station.id, None)
								self.select_route(route)
								####
								log_station_out(new_station)
					except:
						pass
					self.select_station(station)
					###
					log_route_out(route)

		station.visited = 2

	def open_full_station_list(self):
		open_full_station_list_btns = self.driver.find_elements_by_class_name('masstransit-legend-group-view__open-button')
		for btn in open_full_station_list_btns:
			implicit_click(self.driver, btn)

		self.driver.implicitly_wait(1)

	def open_full_route_list(self):
		try:
			open_full_route_list_btn = self.driver.find_element_by_css_selector('.masstransit-vehicles-open-button-view._wide')
			implicit_click(self.driver, open_full_route_list_btn)
			self.driver.implicitly_wait(1)
		except:
			pass
		

	def select_route(self, route):
		try:
			route_btn = self.driver.find_element(By.XPATH, XPATH_ROUTE_BTN_QEURY % (route.get_local_id()))
			implicit_click(self.driver, route_btn)

			WebDriverWait(self.driver, DEFAULT_INTERACTIVE_TIMEOUT) \
				.until(expected_conditions.presence_of_element_located((By.CLASS_NAME, 'masstransit-card-header-view__title')))

			self.open_full_station_list()

			return True
		except Exception as e:
			print(str(e))
			return False


	def select_station(self, station):
		try:
			# try: found station button
			try:            
				station_btn = self.driver.find_element(By.XPATH, XPATH_STATION_BTN_QUERY % (station.id))
			except:
			# except: switch direction and try again
				switch_open_btn = self.driver.find_element_by_class_name('masstransit-card-header-view__another-threads')
				implicit_click(self.driver, switch_open_btn)
				
				switch_btns = self.driver.find_elements(By.XPATH, XPATH_THREAD_BTN_QUERY)
				for switch_btn in switch_btns:
					implicit_click(self.driver, switch_btn)
					self.open_full_station_list()
					try:
						station_btn = self.driver.find_element(By.XPATH, XPATH_STATION_BTN_QUERY % (station.id))
						break
					except:
						station_btn = None
						pass
			
			assert(station_btn != None)
			implicit_click(self.driver, station_btn)

			WebDriverWait(self.driver, DEFAULT_INTERACTIVE_TIMEOUT) \
				.until(expected_conditions.presence_of_element_located((By.CLASS_NAME, 'card-title-view__title')))

			self.open_full_route_list()

			return True
		except Exception as e:
			print(str(e))
			return False




def recursive_dict(obj):
	if type(obj) is list:
		return [recursive_dict(e) for e in obj]
	data = {}
	if type(obj) is dict:
		items = obj.items()
	else:
		items = obj.__dict__.items()
	for key, value in items:
		try:            
			data[key] = recursive_dict(value)
		except AttributeError:
			data[key] = value
	return data

def save_all(stations, routes):
	now = datetime.now()
	output_folder = 'output/%s/' % (now.strftime('%d_%m_%Y_%H_%M_%S'))
	os.makedirs(output_folder, exist_ok=True)

	with open(output_folder + 'routes.json', 'w') as routes_output:
		routes_output.write(json.dumps([recursive_dict(routes[r]) for r in routes]))

	with open(output_folder + 'stations.json', 'w') as stations_output:
		stations_output.write(json.dumps([recursive_dict(stations[s]) for s in stations]))


if __name__ == "__main__":
	city = input('City: ')
	station = input('Station: ') 
	fetch_routes(city, station)






