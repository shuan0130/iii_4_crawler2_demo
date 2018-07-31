def state_code_compute(x):
	from math import cos
	scale_km = 0.5
	lat = x['lat']
	lon = x['long']
	lat_to_km = lat*110.574
	lon_to_km = abs(lon*(111.320*cos(lat)))
	state_code_x = str(int(lat_to_km//scale_km))
	state_code_y = str(int(lon_to_km//scale_km))
	if len(state_code_y) == 4:
		state_code_y = '0'+state_code_y
	state_code = state_code_x+state_code_y
	return state_code

