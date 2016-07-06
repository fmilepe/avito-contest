from mongo_database import *
import csv

# Insere no DB a collection 'label' com as informações do arquivo 'path'
def inserirNoBanco(path, db, label):
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

# Importa os arquivos de teste
def initDatabaseTest():
	print("Importando ItemPairs_Test e ItemInfo_Test para o banco")

	data_itemPairs_test = "data/test/ItemPairs_test.csv"
	data_itemInfo_test = "data/test/ItemInfo_test.csv"
	client = openMongoConnection()
	db = client[DB_NAME]
	db.drop_collection(ITEM_TEST_COLL)
	db.drop_collection(PAIR_TEST_COLL)

	inserirNoBanco(data_itemPairs_test, db, PAIR_TEST_COLL)
	inserirNoBanco(data_itemInfo_test, db, ITEM_TEST_COLL)

	client.close()

# Importa os arquivos de treino
def initDatabase():
	print("Importando ItemPairs e ItemInfo para o banco")

	data_category = "data/Category.csv"
	data_location = "data/Location.csv"
	data_itemPairs_train = "data/train/ItemPairs_train.csv"
	data_itemInfo_train = "data/train/ItemInfo_train.csv"

	client = openMongoConnection()
	db = client[DB_NAME]
	db.drop_collection(CATEGORY_COLL)
	db.drop_collection(LOCATION_COLL)
	db.drop_collection(ITEM_COLL)
	db.drop_collection(PAIR_COLL)

	inserirNoBanco(data_category, db, CATEGORY_COLL)
	inserirNoBanco(data_location, db, LOCATION_COLL)
	inserirNoBanco(data_itemInfo_train, db, ITEM_COLL)
	inserirNoBanco(data_itemPairs_train, db, PAIR_COLL)

	client.close()

# Cria indixes auxiliares para otimizar cálculos posteriores
def createIndexes():
	client = openMongoConnection()
	db = client[DB_NAME]

	print("Criando index para ItemID em", ITEM_COLL)
	db[ITEM_COLL].create_index([("ItemID", pymongo.ASCENDING)], unique=True)
	print("Criando index para ItemID em", ITEM_TEST_COLL)
	db[ITEM_TEST_COLL].create_index([("ItemID", pymongo.ASCENDING)], unique=True)

	client.close()