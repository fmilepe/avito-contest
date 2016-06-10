import numpy as np
from io import StringIO

category_csv = open("Category.csv")
#s = StringIO()
categories = {}
#for column in category_csv[0].split(","):

i =0
for line in category_csv:
    if i == 0:
        for column in line.split(","):
            categories[column.strip()] = []
    else:
        data = line.split(",")
        categories[data[0].strip()] = {}
        categories[data[0].strip()]["parentCategoryID"] = int(data[1].strip())
    i+=1

#print (categories)

location_csv = open("Location.csv")
locations = {}
i =0
for line in location_csv:
    if i == 0:
        for column in line.split(","):
            locations[column.strip()] = []
    else:
        data = line.split(",")
        locations[data[0].strip()] = {}
        locations[data[0].strip()]["regionID"] = int(data[1].strip())
    i+=1

#print(locations)

pairs_csv = open("ItemPairs_train.csv")
pairs = {}
i =0
pairs["n_duplicates"] = 0
for line in pairs_csv:
    if i == 0:
        for column in line.split(","):
            pairs[column.strip()] = []
    else:
		data = line.split(",")
		pairs["itemID_1"].append(int(data[0].strip()))
		pairs["itemID_2"].append(int(data[1].strip()))
		pairs["isDuplicate"].append(int(data[2].strip()))
		if int(data[2].strip()) == 1 :
			pairs["n_duplicates"] += 1
		pairs["generationMethod"].append(int(data[3].strip()))
    i+=1

pairs["n_examples"] = len(pairs["isDuplicate"])

print((pairs["n_duplicates"] * 1.0)/pairs["n_examples"],"% eventos duplicados no dataset")

import csv 
with open("ItemInfo_train.csv","rb") as items:
	for item in items:
		print(item)
