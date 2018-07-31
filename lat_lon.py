def geoc(x, key_num1):
	import pandas as pd
	import numpy as np
	import requests
	from bs4 import BeautifulSoup
	import time
	key1 = 'AIzaSyC-NUP-6jxk8jQIlAc4z0e-m2RDMnJsfM'
	key2 = 'AIzaSyCKSOUbmMAcF_9RyZS-wFU74fPwJUHdN8o'
	key3 = 'AIzaSyBvYksr15gUXBedtFlEBH1RQqL05kj4CRI'
	key4 = 'AIzaSyCdST6Pb1KadM1F2r1MH4d_ySFPOKSIpYg'
	key5 = 'AIzaSyBfGSH2Mn6kIVciwPvCkzU8qD6G6N5yCTM'
	key6 = 'AIzaSyBBy5KU0Ea5uw4ly5aiCyyWTt7b97-JM6M'
	key7 = 'AIzaSyCmzr4zXw0JifMK5_BNG3PKzYDox4ATF14'
	key8 = 'AIzaSyAacwSxLi5_KbvwCQ4E76fIvKpP8kqbnx0'
	key9 = 'AIzaSyCgULGix6zOYlVYbP64D1sIZwKpyiHyXJg'
	key10 = 'AIzaSyAd0p4h1R7Vb-pnWF3DMdwHVxpAyyMHdKs'
	key11 = 'AIzaSyDmb2Bm6RFFcd_7T5E1NuVewdlaF6dSNHo'
	key12 = 'AIzaSyAhPBiGJ6-0CJUu-oVWNcLxlUjKcw6d8eg'
	key13 = 'AIzaSyCykc0kco2cLUVrtJ5_qbnjWJ1C9PUHUwA'
	key14 = 'AIzaSyCyVEdL6zR71c3Gl0w7QLvStgzEhvLMltk'
	key15 = 'AIzaSyBkJgKPc83TYdUJDmOPPc-QergZqFUWvXc'
	key16 = 'AIzaSyB1DhoIukRqpck7oCvejzY3v6GXbdInDe8'
	key_list = [key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15, key16]
	if 'lat' not in x.columns:
		x['lat'] = pd.Series(np.zeros(len(x)), index=x.index)
		x['long'] = pd.Series(np.zeros(len(x)), index=x.index)
	print("the current key number is " + str(key_num1))
	i = 0
	global keyStatus
	keyStatus = 1
	while i < len(x):
		addr = x.iloc[i][1]
		url = 'https://maps.googleapis.com/maps/api/geocode/xml?address=' + addr + '&key=' + key_list[int(key_num1)]
		r = requests.get(url)
		content = r.content
		bsobj = BeautifulSoup(content, 'html.parser')
		status = bsobj.find('status').get_text()
		if status == 'OVER_QUERY_LIMIT':
			print('need to change key, the current file number is ' + str(i))
			key_num1 = int(key_num1) + 1
			print('key number has changed to ' + str(key_num1))
			i+=1
			if key_num1 >= len(key_list):
				keyStatus = 0
				break
		elif status == 'OK':
			lat = bsobj.find_all('lat')[0].get_text()    
			long = bsobj.find_all('lng')[0].get_text()
			x['lat'][i] = lat
			x['long'][i] = long
			i+=1
		else:
			print('address is not vlaid, the current file number is ' + str(i))
			lat = 0
			long = 0
			x['lat'][i] = lat
			x['long'][i] = long
		print(i)
		#print('緯度為: ' + str(lat))
		#print('經度為: ' + str(long))
		time.sleep(1)        
	return x