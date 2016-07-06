import jellyfish as jf
import json
from mongo_database import *
from util import *

# Retorna a proporção entre os precos
def feature_diffPrice(price1, price2):
	if(price1 == '' and price2 == ''):
		return 1
	if(price1 == ''):
		price1 = price2
	if(price2 == ''):
		price2 = price1

	return min((float(price1)/float(price2)), (float(price2)/float(price1)))

# Retorna o inverso da distancia (1/dist) entre as coordenadas em metros
def feature_diffLatLon(lon1,lat1,lon2,lat2):
	lon1 = float(lon1)
	lat1 = float(lat1)
	lon2 = float(lon2)
	lat2 = float(lat2)
	dist = int(haversine(lon1, lat1, lon2, lat2)*1000)
	if(dist < 1):
		return 1
	else:
		return 1/dist

# Retorna em um array de 2 posições: 
# 0: a semelhança média de String(jaro_winkler) entre os campos JSON com keys iguais
# 1: proporção de keys iguais
def feature_attrJson(item1, item2):
	if(item1['attrsJSON'] == '' or item2['attrsJSON'] == ''):
		return [0,0]

	json1 = json.loads(item1['attrsJSON'])
	json2 = json.loads(item2['attrsJSON'])
	soma = 0.0
	n = 0.0
	i = 0.0
	for key in json1.keys():
		i = i + 1.0
		if(json2.get(key, False) != False):
			soma = soma + jf.jaro_winkler(json2[key],json1[key])
			n = n + 1.0

	if(n > 0):
		return [soma/n, n/i]
	else:
		return [0,0]

# Cria um JSON com as features 'x'. O método é "geral", criando features contínuas e discretas.
def mergeItemsGeneral(item1, item2):
	diff_price = feature_diffPrice(item1['price'], item2['price'])
	diff_latlon = feature_diffLatLon(item1['lon'], item1['lat'], item2['lon'], item2['lat'])
	simi_json,eq_keys_json = feature_attrJson(item1, item2)
	
	json = {
				'x': 
				[
					int(item1['categoryID'] == item2['categoryID']),
					int(item1['locationID'] == item2['locationID']),
					int(item1['metroID'] 	== item2['metroID']),
					int(item1['metroID'] == ''),
					int(item2['metroID'] == ''),
					int(item1['price']		== item2['price']),
					int(item1['price'] 		== ''),
					int(item2['price'] 		== ''),
					int(item1['price'] 		== '1.0'),
					int(item2['price'] 		== '1.0'),
					diff_price,
					jf.jaro_winkler(item1['title'], item2['title']),
					jf.jaro_winkler(item1['description'], item2['description']),				
					int(item1['attrsJSON'] == ''),
					int(item2['attrsJSON'] == ''),
					diff_latlon,
					simi_json,
					eq_keys_json
				]
	}
	# no for seguinte, estou assumindo que os dois itens tem a mesma categoria sempre!
	for cat in categories.keys():
		if(int(item1['categoryID']) == cat):
			json['x'].append(1)
		else:
			json['x'].append(0)


	return json

def discretizaDiffPrice(diff_price):
	if(diff_price <= 0.01):
		return 1
	elif(diff_price <= 0.05 ):
		return 2
	elif(diff_price <= 0.1):
		return 3
	elif(diff_price <= 0.2):
		return 4
	elif(diff_price <= 0.3):
		return 5
	elif(diff_price <= 0.4):
		return 6
	elif(diff_price <= 0.5):
		return 7
	elif(diff_price <= 0.8):
		return 8
	elif(diff_price <= 1.0):
		return 9
	else:
		return 10

def mergeItemsDiscretes(item1, item2):
	diff_price = feature_diffPrice(item1['price'], item2['price'])
	diff_price = discretizaDiffPrice(diff_price)
	jw_title = jf.jaro_winkler(item1['title'], item2['title'])
	jw_desc  = jf.jaro_winkler(item1['description'], item2['description'])
	simi_json,eq_keys_json = feature_attrJson(item1, item2)
	diff_latlon = feature_diffLatLon(item1['lon'], item1['lat'], item2['lon'], item2['lat'])

	metroID1 = '0'
	if(item1['metroID'] != ''):
		metroID1 = item1['metroID']

	metroID2 = '0'
	if(item2['metroID'] != ''):
		metroID2 = item2['metroID']

	json = {
		'x':
		[
			int(item1['itemID'] == item2['itemID']),
			
			int(item1['categoryID']),
			int(item2['categoryID']),
			int(item1['categoryID'] == item2['categoryID']),

			int(item1['metroID'] == ''),
			int(item2['metroID'] == ''),
			int(round(float(metroID1))),
			int(round(float(metroID2))),
			int(item1['metroID'] == item2['metroID']),

			int(item1['locationID'] == ''),
			int(item2['locationID'] == ''),
			int(item1['locationID']),
			int(item2['locationID']),
			int(item1['locationID'] == item2['locationID']),

			int(item1['price']	== item2['price']),
			int(item1['price'] 	== ''),
			int(item2['price'] 	== ''),
			int(item1['price'] 	== '1.0'),
			int(item2['price'] 	== '1.0'),
			diff_price,

			round(jw_title * 10),
			round(jw_desc * 10),

			int(item1['attrsJSON'] == ''),
			int(item2['attrsJSON'] == ''),
			round(round(simi_json * 100)/10),
			round(round(eq_keys_json * 100)/10),

			round(((1/diff_latlon)/1000))
		]
	}

	return json

# Cria as features de treinamento usando valores gerais: discretos e contínuos
def initFeaturesGeneral():
	print("(Re)inicializando features gerais de treinamento")
	client = openMongoConnection()
	
	db = client[DB_NAME]
	db.drop_collection(FEATURES_COLL)
	features = db[FEATURES_COLL]
	cursor = db[PAIR_ITEM_COLL].find({})
	bulk = []
	total = cursor.count()
	i = 0

	for doc in cursor:
		if(doc.get('isDuplicate', False) == False):
			continue

		json = mergeItemsGeneral(doc['item1'], doc['item2'])
		json['y'] = int(doc['isDuplicate'])

		bulk.append(json)
		i = i + 1

		if(len(bulk) >= BULK_LIMIT):
			features.insert(bulk)
			bulk = []
			print(i, "de", total)

	if(len(bulk) > 0):
		features.insert(bulk)

	client.close()

# Cria as features de teste usando valores gerais: discretos e contínuos
def initFeaturesGeneralTest():
	print("(Re)inicializando features gerais de test")
	client = openMongoConnection()

	db = client[DB_NAME]
	db.drop_collection(FEATURES_TEST_COLL)
	features = db[FEATURES_TEST_COLL]
	cursor = db[PAIR_ITEM_TEST_COLL].find({})
	bulk = []
	total = cursor.count()
	i = 0

	for doc in cursor:
		json = mergeItemsGeneral(doc['item1'], doc['item2'])

		bulk.append(json)
		i = i + 1

		if(len(bulk) >= BULK_LIMIT):
			features.insert(bulk)
			bulk = []
			print(i, "de", total)

	if(len(bulk) > 0):
		features.insert(bulk)

	client.close()

# Features de treino para usar no XGBoost
def initFeaturesDiscretes():
	print("(Re)inicializando features discretas de treino")
	client = openMongoConnection()
	
	db = client[DB_NAME]
	db.drop_collection(FEATURES_XGB_COLL)
	features = db[FEATURES_XGB_COLL]
	cursor = db[PAIR_ITEM_COLL].find({})
	bulk = []
	total = cursor.count()
	i = 0

	for doc in cursor:
		if(doc.get('isDuplicate', False) == False):
			continue

		json = mergeItemsDiscretes(doc['item1'], doc['item2'])
		json['y'] = int(doc['isDuplicate'])

		bulk.append(json)
		i = i + 1

		if(len(bulk) >= BULK_LIMIT):
			features.insert(bulk)
			bulk = []
			print(i, "de", total)

	if(len(bulk) > 0):
		features.insert(bulk)

	client.close()

# Features de teste para usar no XGBoost
def initFeaturesDiscretesTest():
	print("(Re)inicializando features discretas de test")
	client = openMongoConnection()

	db = client[DB_NAME]
	db.drop_collection(FEATURES_XGB_TEST_COLL)
	features = db[FEATURES_XGB_TEST_COLL]
	cursor = db[PAIR_ITEM_TEST_COLL].find({})
	bulk = []
	total = cursor.count()
	i = 0

	for doc in cursor:
		json = mergeItemsDiscretes(doc['item1'], doc['item2'])

		bulk.append(json)
		i = i + 1

		if(len(bulk) >= BULK_LIMIT):
			features.insert(bulk)
			bulk = []
			print(i, "de", total)

	if(len(bulk) > 0):
		features.insert(bulk)

	client.close()