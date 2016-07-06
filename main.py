from data_load import *
from data_merge import *
from feature_creator import *
import time
from classifiers import *

def tempo(start, task):
	print("T =",round((time.time()-start)/60), "minutos \t", task)

def main():
	"""
	start = time.time()
	initDatabase()
	tempo(start, "initDatabase")
	
	start = time.time()	
	initDatabaseTest()
	tempo(start, "initDatabaseTest")
	
	#start = time.time()
	#createIndexes()
	#tempo(start, "createIndexes")
	
	start = time.time()
	aggragatePairsItems()
	tempo(start, "aggragatePairsItems")

	start = time.time()
	aggragatePairsItemsTest()
	tempo(start, "aggragatePairsItemsTest")
	
	start = time.time()
	initFeaturesGeneral()
	tempo(start, "initFeaturesGeneral")

	start = time.time()
	initFeaturesGeneralTest()
	tempo(start, "initFeaturesGeneralTest")	
	"""

	start = time.time()
	classify(learn_xgb=True)
	tempo(start, "classify")

main()