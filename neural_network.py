import numpy as np
import csv
import sys
import time
from pymongo import MongoClient
from geopy.distance import vincenty
import jellyfish as jf
from sklearn.neural_network import MLPClassifier
from sklearn import cross_validation
from sklearn.preprocessing import StandardScaler
from sklearn.cross_validation import KFold
from sklearn.preprocessing import normalize
import json
import random

BULK_LIMIT = 1000
DB_NAME = 'kaggle'
FEATURES_COLL = 'features'
FEATURES_TEST_COLL = 'features_test'
ITEM_COLL = 'item'
CATEGORY_COLL = 'category'
LOCATION_COLL = 'location'
PAIR_COLL = 'pair'
ITEM_TEST_COLL = 'item_test'
PAIR_TEST_COLL = 'pair_test'
PAIR_ITEM_COLL = 'pair_item'
PAIR_ITEM_TEST_COLL = 'pair_item_test'

categories = {}

def openMongoConnection():
	client = MongoClient('localhost', 27017)
	return client

def lerArquivo(path, db, label):
	i = 0
	fieldnames = []
	collection = db[label]

	bulk = []

	with open(path) as csvfile:
		datareader = csv.reader(csvfile)
		
		for values in datareader:
			if(i == 0):
				fieldnames = values
			else:
				json = {'row_number': (i-1) }
				j = 0

				for field in fieldnames:
					if(field == 'itemID' or field == 'itemID_1' or field == 'itemID_2'):
						values[j] = int(values[j])
					json[field] = values[j]
					j = j + 1


				bulk.append(json)
				if(len(bulk) >= BULK_LIMIT):
					collection.insert(bulk)
					bulk = []
			i = i + 1

	if(len(bulk) > 0):
		collection.insert(bulk)

	print(i-1, label)

def initDatabaseTest():
	print("(Re)inicializando db kaggle/testes")

	data_itemPairs_test = "data/test/ItemPairs_test.csv"
	data_itemInfo_test = "data/test/ItemInfo_test.csv"
	client = openMongoConnection()
	db = client[DB_NAME]
	db.drop_collection(ITEM_TEST_COLL)
	db.drop_collection(PAIR_TEST_COLL)

	lerArquivo(data_itemPairs_test, db, PAIR_TEST_COLL)
	lerArquivo(data_itemInfo_test, db, ITEM_TEST_COLL)

def initDatabase():
	print("(Re)inicializando db kaggle")

	data_category = "data/Category.csv"
	data_location = "data/Location.csv"
	data_itemPairs_train = "data/train/ItemPairs_train.csv"
	data_itemInfo_train = "data/train/ItemInfo_train.csv"

	client = openMongoConnection()
	client.drop_database(DB_NAME)
	db = client[DB_NAME]

	lerArquivo(data_category, db, CATEGORY_COLL)
	lerArquivo(data_location, db, LOCATION_COLL)
	lerArquivo(data_itemInfo_train, db, ITEM_COLL)
	lerArquivo(data_itemPairs_train, db, PAIR_COLL)

	#db[ITEM_COLL].create_index("ItemID")

	client.close()

def initPairItem():
	print("Preenchendo merge collection PairItem(Pair+Item) de row_number inexistente")
	client = openMongoConnection()
	db = client[DB_NAME]
	cursor = db.pair.find({})
	count = cursor.count()
	itemCollection = db[ITEM_COLL]

	start = time.time()
	bulk = db[PAIR_ITEM_COLL].initialize_ordered_bulk_op()
	i = 0
	b = 0
	tempo = 0
	for doc in cursor:
		if(doc.get('row_number', False) == False):
			print(doc)

		existingCount = db[PAIR_ITEM_COLL].find({
								'row_number': doc['row_number']
								}).limit(1).count(True)

		if(existingCount == 0):
			print("Não existe:",doc['itemID_1'],doc['itemID_2'])
			item1 = itemCollection.find_one({'itemID': doc['itemID_1']})
			item2 = itemCollection.find_one({'itemID': doc['itemID_2']})
			doc['item1'] = item1
			doc['item2'] = item2
			bulk.insert(doc)
			b = b + 1
			if(b >= BULK_LIMIT):
				b = 0
				bulk.execute()
				bulk = db[PAIR_ITEM_COLL].initialize_ordered_bulk_op()
				print(i,"de",count,"em",(time.time()-start),"segundos")
				start = time.time()
				tempo = 0

		i = i + 1
		if(i % 10000 == 0):
			print(i,"de",count)

	if(b > 0):
		bulk.execute()

	client.close()

def initPairItemTest():
	print("(Re)inicializando merge collection PairItemTest(Pair+Item) de row_number inexistente")
	client = openMongoConnection()
	db = client[DB_NAME]
	db.drop_collection(PAIR_ITEM_TEST_COLL)
	cursor = db.pair_test.find({})
	count = cursor.count()
	itemCollection = db[ITEM_TEST_COLL]

	start = time.time()
	bulk = []
	i = 0
	tempo = 0

	for doc in cursor:
		item1 = itemCollection.find_one({'itemID': doc['itemID_1']})
		item2 = itemCollection.find_one({'itemID': doc['itemID_2']})
		doc['item1'] = item1
		doc['item2'] = item2
		bulk.append(doc)

		i = i + 1
		if(len(bulk) >= BULK_LIMIT):
			db[PAIR_ITEM_TEST_COLL].insert(bulk)
			bulk = []
			print(i,"de",count,"em",(time.time()-start),"segundos")
			start = time.time()
			tempo = 0

	if(len(bulk) > 0):
		db[PAIR_ITEM_TEST_COLL].insert(bulk)

	client.close()

def feature_diffPrice(price1, price2):
	if(price1 == '' and price2 == ''):
		return 1
	if(price1 == ''):
		price1 = price2
	if(price2 == ''):
		price2 = price1

	return max((float(price1)/float(price2)), (float(price2)/float(price1)))

def feature_diffLatLon(lon1,lat1,lon2,lat2):
	lon1 = float(lon1)
	lat1 = float(lat1)
	lon2 = float(lon2)
	lat2 = float(lat2)
	place1 = (lat1, lon1)
	place2 = (lat2, lon2)
	dist = int(vincenty(place1, place2).meters)
	if(dist == 0):
		return 1
	else:
		return 1/dist

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

def mergeItems(item1, item2):
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

	if(item1['categoryID'] != item2['categoryID']):
		print('CATEGORIAS DIFERENTES:',item1['itemID'],item2['itemID'])
	# no for seguinte, estou assumindo que os dois itens tem a mesma categoria sempre!
	for cat in categories.keys():
		if(int(item1['categoryID']) == cat):
			json['x'].append(1)
		else:
			json['x'].append(0)
	#	if(int(item2['categoryID']) == cat):
	#		json['x'].append(1)
	#	else:
	#		json['x'].append(0)


	return json

def initFeaturesTest():
	print("(Re)inicializando features/test")
	client = openMongoConnection()

	db = client[DB_NAME]
	db.drop_collection(FEATURES_TEST_COLL)
	features = db[FEATURES_TEST_COLL]
	cursor = db[PAIR_ITEM_TEST_COLL].find({})
	bulk = []
	total = cursor.count()
	i = 0

	for doc in cursor:
		json = mergeItems(doc['item1'], doc['item2'])

		bulk.append(json)
		i = i + 1

		if(len(bulk) >= BULK_LIMIT):
			features.insert(bulk)
			bulk = []
			print(i, "de", total)

	if(len(bulk) > 0):
		features.insert(bulk)

	client.close()

def initFeatures():
	print("(Re)inicializando features")
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

		json = mergeItems(doc['item1'], doc['item2'])
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

def classify():
	client = openMongoConnection()
	db = client[DB_NAME]

	cursor = db[FEATURES_COLL].find({})
	X = []
	y = []
	for doc in cursor:
		X.append(doc['x'])
		y.append(int(doc['y']))

	X = np.array(X)
	y = np.array(y)

	X = normalize(X)

	clf = MLPClassifier(hidden_layer_sizes=(100,50), activation='tanh', algorithm='adam', alpha=1e-5,
						learning_rate='constant',tol=1e-8,learning_rate_init=0.0002,
						early_stopping=True,validation_fraction=0.2)
	
	kf = KFold(len(y),n_folds=3)

	print("Iniciando treinamento")
	i = 0
	for train,test in kf:
		start = time.time()
		i = i + 1
		print("Treinamento",i)

		# dividindo dataset em treino e test
		#X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.4, random_state=1)
		X_train, X_test, y_train, y_test = X[train], X[test], y[train], y[test]
		
		# fit
		clf.fit(X_train, y_train)
		print("score:",clf.score(X_test, y_test),"(",(time.time()-start)/60.0,"minutos )")

	X_Kaggle = []
	cursor = db[FEATURES_TEST_COLL].find({})
	for doc in cursor:
		X_Kaggle.append(doc['x'])

	predict = clf.predict_proba(X_Kaggle)
	
	i = 0
	f = open('output','w')
	f.write("id,probability\n")
	for p in predict:
		f.write(str(i)+","+str(p[1])+"\n")
		i = i + 1

	client.close()

def main():
	start_time = time.time()
	args = {}

	client = openMongoConnection()
	db = client[DB_NAME]
	cursor = db.category.find({}).sort("categoryID")

	for c in cursor:
		categories[int(c['categoryID'])] = int(c['parentCategoryID'])

	for arg in sys.argv:
		args[arg] = True

	if(args.get("initdb", False) is True):
		print("initdb")
		#initDatabase()
		initPairItem()

	if(args.get("initdbtest", False) is True):
		print("initdbtest")
		#initDatabaseTest()
		initPairItemTest()

	if(args.get("initfeaturestest", False) is True):
		print("initfeaturestest")
		initFeaturesTest()

	if(args.get("initfeatures", False) is True):
		print("initfeatures")
		initFeatures()

	if(args.get("classify", False) is True):
		print("classify")
		classify()

	print("--- %s minutos ---" % ((time.time() - start_time)/60.0))

main()