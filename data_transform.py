import numpy as np
from io import StringIO

category_csv = open("Category.csv")
categories = {}

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

print(pairs["n_duplicates"] * 1.0)/pairs["n_examples"],"% pares duplicados no dataset"

items = {}

import csv 
with open("ItemInfo_train.csv","rb") as arq:
	i = 0
	items_data = csv.reader(arq,delimiter=',')
	m_ids = []
	for item in items_data:
		if i > 0 :
			items[item[0]] = {}
			items[item[0]]["categoryID"] = item[1]
			items[item[0]]["title"] = item[2]
			items[item[0]]["description"] = item[3]
			items[item[0]]["images_array"] = item[4].split(',')
			items[item[0]]["price"] = item[6]
			items[item[0]]["locationID"] = item[7]
			items[item[0]]["metroID"] = item[8]
			items[item[0]]["lat"] = item[9]
			items[item[0]]["lon"] = item[10]
			m_ids.append(item[8])
			print (i)
		if i > 200000:
			break
		i += 1
	print "Numero de metroID's diferentes : ", len(set(m_ids))

def transform_data_discrete_features(items,pairs,locations,categories):
	""" 
		Funcao que pre-processa os dados e gera um conjunto de dados somente com features discretas.
	"""
	merged_data = []
	y = []
	for pair in pairs :
		y.append(pair["isDuplicate"])
		
		#Cria linha que sera adicionada a matriz de dados
		line = []
		
		#Carrega os dois itens para variaveis locais
		item1 = items[pair["itemID_1"]]
		item2 = items[pair["itemID_2"]]
		
		#Calcula campo booleano que diz se os itens sao de mesma categoria
		if item1["categoryID"] == item2["categoryID"] :
			same_category = 1
		else :
			same_category = 0
		line.append(same_category)
		
		#Calcula campo booleano que diz se a categoria dos itens faz parte da mesma subcategoria
		if categories[item1["categoryID"]]["parentCategoryID"] == categories[item2["categoryID"]]["parentCategoryID"] :
			same_parent_category = 1
		else :
			same_parent_category = 0
		line.append(same_parent_category)
		
		#Calcula campo que diz qual a diferenca entre o preco de 2 itens anunciados
		price_diff_percent = abs(float(item1["price"]) - float(item2["price"]))/max(float(item1["price"]),float(item2["price"]))
		price_diff = 0
		if price_diff_percent == 0:
			price_diff = 0
		elif price_diff_percent < 1:
			price_diff = 1
		elif price_diff_percent < 2:
			price_diff = 2
		elif price_diff_percent < 3:
			price_diff = 3
		elif price_diff_percent < 4:
			price_diff = 4
		elif price_diff_percent < 5:
			price_diff = 5
		elif price_diff_percent < 10:
			price_diff = 6
		else:
			price_diff = 7
		line.append(price_diff)
		
		#Calcula campo booleano que diz se o location e o mesmo
		if item1["locationID"] == item2["locationID"] :
			same_location = 1
		else :
			same_location = 0
		line.append(same_location)
		
		#Calcula campo booleano que diz se a regiao do location e o mesmo
		if locations[item1["locationID"]]["regionID"] == locations[item2["locationID"]]["regionID"] :
			same_region = 1
		else :
			same_region = 0
		line.append(same_region)

		#Calcula campo booleano que diz se o metro (seja la o que isso for) e o mesmo
		if item1["metroID"] == item2["metroID"] :
			same_metro = 1
		else :
			same_metro = 0
		line.append(same_metro)
	
	merged_data.append(line)
	
	return merged_data

def transform_data_non_discrete_features(items,pairs,locations,categories):
	pass
