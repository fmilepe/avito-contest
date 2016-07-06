from mongo_database import *
import time

# Agrega as informações de pares de treino fazendo append de Item1 e Item2 no documento
def aggragatePairsItems():
	print("(Re)inicializando Merge (Pair+Item1+Item2)")
	client = openMongoConnection()
	db = client[DB_NAME]
	db.drop_collection(PAIR_ITEM_COLL)
	cursor = db.pair.find({})
	count = cursor.count()
	itemCollection = db[ITEM_COLL]

	start = time.time()
	bulk = []
	i = 0

	for doc in cursor:
		item1 = itemCollection.find_one({'itemID': doc['itemID_1']})
		item2 = itemCollection.find_one({'itemID': doc['itemID_2']})
		doc['item1'] = item1
		doc['item2'] = item2
		bulk.append(doc)
		
		i = i + 1
		if(len(bulk) >= BULK_LIMIT):
			db[PAIR_ITEM_COLL].insert(bulk)
			bulk = []
			print(i,"de",count,"em",(time.time()-start),"segundos")
			start = time.time()
	
	if(len(bulk) > 0):
		db[PAIR_ITEM_COLL].insert(bulk)

	db[PAIR_ITEM_COLL].createIndex("row_number")

	client.close()


# Agrega as informações de pares de teste fazendo append de Item1 e Item2 no documento
def aggragatePairsItemsTest():
	print("(Re)inicializando Merge Test (Pair+Item1+Item2)")
	client = openMongoConnection()
	db = client[DB_NAME]
	db.drop_collection(PAIR_ITEM_TEST_COLL)
	cursor = db.pair_test.find({})
	count = cursor.count()
	itemCollection = db[ITEM_TEST_COLL]

	start = time.time()
	bulk = []
	i = 0

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

	if(len(bulk) > 0):
		db[PAIR_ITEM_TEST_COLL].insert(bulk)

	client.close()