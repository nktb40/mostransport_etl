from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.proxy import Proxy, ProxyType

from enum import Enum

DEFAULT_INTERACTIVE_TIMEOUT = 60

class TransportType(Enum):
	TRAM = 0
	METRO = 1
	RAIL = 2
	BUS = 3
	FERRY = 4
	CABLE_TRAM = 5
	AERIAL_LIFT = 6
	FUNICULAR = 7
	TROLLEYBUS = 11
	MONORAIL = 12

		
TRANSPORT_TYPES = {
	# gtfs_code name labels description
	TransportType.TRAM: (0, ['трамвай', 'tram', 'streetcar', 'LRT', 'light rail'], 'Any light rail or street level system within a metropolitan area'),
	1: (1, ['метро', 'subway', 'metro'], 'Any underground rail system within a metropolitan area'),
	2: (2, ['электричка', 'поезд', 'rail'], 'Used for intercity or long-distance travel'),
	3: (3, ['автобус', 'bus'], 'Used for short- and long-distance bus route'),
	4: (4, ['паром', 'катер', 'водный транспорт', 'ferry'], 'Used for short- and long-distance boat service'),
	5: (5, ['канатный трамвай', 'cable tram'], 'Used for street-level rail cars where the cable runs beneath the vehicle, e.g., cable car in San Francisco'),
	6: (6, ['канатная дорога', 'aerial lift'], 'Cable transport where cabins, cars, gondolas or open chairs are suspended by means of one or more cables'),
	7: (7, ['фуникулер', 'funicular'], 'Any rail system designed for steep inclines'),
	11: (11, ['троллейбус', 'trolleybus'], 'Electric buses that draw power from overhead wires using poles'),
	12: (12, ['монорельс', 'monorail'], 'Railway in which the track consists of a single rail or a beam')
}

def parse_transport_type(label):
	for transport_type in TRANSPORT_TYPES.values():
		if lable.strip().lower() in transport_type[1]:
			return transport_type
	return None

def get_transport_type(gtfs_code):
	if not gtfs_code in TRANSPORT_TYPES:
		return None
	return TRANSPORT_TYPES[gtfs_code]

# Инициируем WebDriver
def create_driver(proxy_flag):
	options = Options()
	options.headless = False
	options.add_argument("--window-size=1920,1200")

	if proxy_flag == True:
		proxy = get_proxy()
		options.add_argument('--proxy-server='+proxy['addr'])
		proxy['state']='used'

	DRIVER_PATH = '/usr/bin/chromedriver'

	driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)
	
	return driver

proxy_list = []

def get_proxy():
	global proxy_list

	if len(proxy_list) == 0:
		proxy_list = download_proxy_list()

	proxy = next(filter(lambda x: x['state'] == 'new', proxy_list), None)
	return proxy

# Функция получения списка proxy адресов
def download_proxy_list():
	driver = create_driver(False)
	url = 'https://www.proxy-list.download/HTTP'
	proxy_list = []

	try:
		driver.get(url)

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


def find_interactive_element(driver, by, selector, timeout=DEFAULT_INTERACTIVE_TIMEOUT):
	"""Wait for appearence of element in dom within timeout and then return element or raise 
	exception
	"""
	try:
		WebDriverWait(driver, timeout).until(expected_conditions.presence_of_element_located((by, selector)))
		return driver.find_element(by, selector)
	except:
		raise Exception("Can't find interactive element {}".format(str((by, selector))))

def implicit_click(driver, element):
	"Execute click on element which can be invisible"
	driver.execute_script("return arguments[0].click();", element)