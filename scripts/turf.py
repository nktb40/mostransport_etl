from turfik import distance as turf_dist
from turfik import destination as turf_dest
from turfik import helpers as turf_helpers
from shapely.ops import split, substring, unary_union, nearest_points
from shapely.geometry import *
import math

# Функция обрезания линии между двумя точками
def line_slice(startPt, stopPt, line):
	coords = list(line.coords)
	multi_line = MultiPoint(line.coords)

	if line.type == 'LineString':
		
		startVertex = nearest_points(multi_line, startPt['geometry'])[0].coords[0]
		stopVertex = nearest_points(multi_line, stopPt['geometry'])[0].coords[0]

		start_indx = coords.index(startVertex)
		stop_indx = coords.index(stopVertex)

		ends = []
		if start_indx <= stop_indx:	
			ends = [start_indx, stop_indx]
		else:
			ends = [stop_indx, start_indx]

		clipCoords = coords[ends[0] : ends[1]+1]

		if(len(clipCoords) == 1):
			clipCoords.append(clipCoords[0])

		return LineString(clipCoords)

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

