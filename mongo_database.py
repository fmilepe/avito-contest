import pymongo

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

def openMongoConnection():
	client = pymongo.MongoClient('localhost', 27017)
	return client

def loadCategories():
	categories = {}
	client = openMongoConnection()
	db = client[DB_NAME]
	cursor = db.category.find({}).sort("categoryID")

	for c in cursor:
		categories[int(c['categoryID'])] = int(c['parentCategoryID'])

categories = {}
loadCategories()