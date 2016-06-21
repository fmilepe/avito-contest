#Importando modulos a serem usados no tratamento de dados
import csv 
import numpy as np
import pandas as pd
import json
#import jellyfish as jf

def parse_categories(filepath):
    categories = {}
    i =0
    with open(filepath) as category_file:
        category_csv = csv.reader(category_file,delimiter=",")
        for line in category_csv:
            if i == 0:
                for column in line:
                    categories[column.strip()] = []
            else:
                categories[line[0].strip()] = {}
                categories[line[0].strip()]["parentCategoryID"] = int(line[1].strip())
            i+=1
    print ("[LOG] ",i," categories parsed")
    
    return categories
def parse_locations(filepath):
    locations = {}
    i =0
    with open(filepath) as location_file:
        location_csv = csv.reader(location_file,delimiter=",")
        for line in location_csv:
            if i == 0:
                for column in line:
                    locations[column.strip()] = []
            else:
                locations[line[0].strip()] = {}
                locations[line[0].strip()]["regionID"] = int(line[1].strip())
            i+=1
    print ("[LOG] ",i," locations parsed")
    
    return locations

def parse_pairs(filepath):
    """
        Recebe o nome do arquivo de pares de anuncios por parametro e o carrega para um dicionario com listas para cada campo. Cada lista esta ordenada de acordo com a linha do arquivo original a qual ela se refere.
    """
    pairs = {}
    i =0
    pairs["n_duplicates"] = 0
    with open(filepath) as pairs_file:
        train = False
        pairs_csv = csv.reader(pairs_file,delimiter=',')
        for line in pairs_csv:
            if i == 0:
                for column in line:
                    pairs[column.strip()] = []
            else:
                if "id" in pairs:
                    pairs["id"].append(int(line[0].strip()))
                    pairs["itemID_1"].append(int(line[1].strip()))
                    pairs["itemID_2"].append(int(line[2].strip()))
                else:
                    pairs["itemID_1"].append(int(line[0].strip()))
                    pairs["itemID_2"].append(int(line[1].strip()))
                    if "isDuplicate" in pairs :
                        train = True
                        pairs["isDuplicate"].append(int(line[2].strip()))
                        if int(line[2].strip()) == 1 :
                            pairs["n_duplicates"] += 1
                    if "generationMethod" in pairs:
                        pairs["generationMethod"].append(int(line[3].strip()))
            i+=1
        if "isDuplicate" in pairs:
            pairs["n_examples"] = i
    
    print ("[LOG] ", i, " pairs of ads parsed")
    print ("[DATA-INFO] ",(pairs["n_duplicates"] * 1.0)/i,"% pares duplicados no dataset")
    
    return pairs

def parse_items(filepath):
    items = {}
    with open(filepath,"rt", encoding='utf-8') as arq:
        i = 0
        items_data = csv.reader(arq,delimiter=',')
        m_ids = []
        for item in items_data:
            if i > 0 :
                items[item[0]] = {}
                items[item[0]]["categoryID"] = item[1].strip()
                items[item[0]]["title"] = item[2].strip()
                items[item[0]]["description"] = item[3]
                if not item[6] == "":
                    items[item[0]]["images_array"] = item[4].strip().split(',')
                else:
                    items[item[0]]["images_array"] = []
                if item[5] != "":
                    items[item[0]]["attrsJSON"] = json.loads(item[5].strip())
                else:
                    items[item[0]]["attrsJSON"] = {}
                if not item[6] == "":
                    items[item[0]]["price"] = item[6]
                else:
                    items[item[0]]["price"] = 0
                items[item[0]]["locationID"] = item[7]
                items[item[0]]["metroID"] = item[8]
                items[item[0]]["lat"] = item[9]
                items[item[0]]["lon"] = item[10]
                m_ids.append(item[8])
            i += 1
        print ("[LOG] ",i, " items parsed")   
        print ("[DATA-INFO] Numero de metroID's diferentes : ", len(set(m_ids)))
        
    return items

def transform_data_discrete_features(items,pairs,locations,categories,file_output=False,outfile="merged_data.csv"):
    """ 
        Funcao que pre-processa os dados e gera um conjunto de dados somente com features discretas.
    """
    merged_data = []
    if file_output:
        output = open(outfile,"w")
    y = []
    n_same_metro = 0
    n_same_loc = 0
    n_same_reg = 0
    n_same_categ = 0
    n_same_parent_categ = 0
    n_price_diff = {}
    n_attr_sim = {}
    for i in range(len(pairs["itemID_1"])) :
        if "isDuplicate" in pairs:
            y.append(pairs["isDuplicate"][i])
        
        #Cria linha que sera adicionada a matriz de dados
        line = []
        
        #Adiciona o ID do individuo de teste aos dados na primeira posicao
        if "id" in pairs:
            line.append(str(pairs["id"][i]))
       
        #Carrega os dois itens para variaveis locais
        item1 = items[str(pairs["itemID_1"][i])]
        item2 = items[str(pairs["itemID_2"][i])]
        
        #calcula a distancia de jaro-winkler para os titulos dos anuncios
        #print (round(jf.jaro_winkler(item1["title"],item2["title"] * 1000)))

        #Calcula campo booleano que diz se os itens sao de mesma categoria
        if item1["categoryID"] == item2["categoryID"] :
            same_category = "1"
            n_same_categ += 1
        else :
            same_category = "0"

        line.append(same_category)
        
        #Calcula campo booleano que diz se a categoria dos itens faz parte da mesma subcategoria
        if categories[str(item1["categoryID"])]["parentCategoryID"] == categories[str(item2["categoryID"])]["parentCategoryID"] :
            same_parent_category = "1"
            n_same_parent_categ +=1
        else :
            same_parent_category = "0"
        
        line.append(same_parent_category)
        #Calcula campo que diz qual a diferenca entre o preco de 2 itens anunciados
        if not max(float(item1["price"]),float(item2["price"])) == 0:
            price_diff_percent = abs(float(item1["price"]) - float(item2["price"]))/max(float(item1["price"]),float(item2["price"]))
        else:
            price_diff_percent = 0
        
        price_diff = "0"
        
        if price_diff_percent == 0:
            price_diff = "0"
            if "0" in n_price_diff:
                n_price_diff["0"] += 1
            else:
                n_price_diff["0"] = 1
        elif price_diff_percent < 0.01:
            price_diff = "1"
            if "1" in n_price_diff:
                n_price_diff["1"] += 1
            else:
                n_price_diff["1"] = 1
        elif price_diff_percent < 0.02:
            price_diff = "2"
            if "2" in n_price_diff:
                n_price_diff["2"] += 1
            else:
                n_price_diff["2"] = 1
        elif price_diff_percent < 0.03:
            price_diff = "3"
            if "3" in n_price_diff:
                n_price_diff["3"] += 1
            else:
                n_price_diff["3"] = 1
        elif price_diff_percent < 0.04:
            price_diff = "4"
            if "4" in n_price_diff:
                n_price_diff["4"] += 1
            else:
                n_price_diff["4"] = 1
        elif price_diff_percent < 0.05:
            price_diff = "5"
            if "5" in n_price_diff:
                n_price_diff["5"] += 1
            else:
                n_price_diff["5"] = 1
        elif price_diff_percent < 0.10:
            price_diff = "6"
            if "6" in n_price_diff:
                n_price_diff["6"] += 1
            else:
                n_price_diff["6"] = 1
        else:
            price_diff = "7"
            if "7" in n_price_diff:
                n_price_diff["7"] += 1
            else:
                n_price_diff["7"] = 1
        
        line.append(price_diff)
        
        #Calcula campo booleano que diz se o location e o mesmo
        if item1["locationID"] == item2["locationID"] :
            same_location = "1"
            n_same_loc += 1
        else :
            same_location = "0"
        
        line.append(same_location)
        
        #Calcula campo booleano que diz se a regiao do location e o mesmo
        if locations[item1["locationID"]]["regionID"] == locations[item2["locationID"]]["regionID"] :
            same_region = "1"
            n_same_reg += 1
        else :
            same_region = "0"
        
        line.append(same_region)
        
        #Calcula campo booleano que diz se o metro (seja la o que isso for) e o mesmo
        if item1["metroID"] == item2["metroID"] :
            same_metro = "1"
            n_same_metro += 1
        else :
            same_metro = "0"
        merged_data.append(line)
        line.append(same_metro)
        
        #Calculando semelhanca entre attrsJSON
        n_attrs = float(len(item1["attrsJSON"]))
        common_attrs = 0.0
        for attr in item1["attrsJSON"]:
            if attr in item2["attrsJSON"]:
                if item1["attrsJSON"][attr] == item2["attrsJSON"][attr]:
                    common_attrs += 1
        attrs_similarity = round(((common_attrs+1)/(n_attrs+1))*10)
        if not str(attrs_similarity) in n_attr_sim:
            n_attr_sim[str(attrs_similarity)] = 1
        else:
            n_attr_sim[str(attrs_similarity)] += 1
        line.append(str(attrs_similarity))
        
        if file_output:
            if "isDuplicate" in pairs:
                line.append(str(pairs["isDuplicate"][i]))
            csv_line = ",".join(line)
            csv_line += "\n"
            output.write(csv_line)
        merged_data.append(line)
    if file_output:
        output.close()
    print("[DATA-INFO] Numero de metroID duplicados :",n_same_metro)
    print("[DATA-INFO] Numero de location duplicados :",n_same_loc)
    print("[DATA-INFO] Numero de regionID duplicados :",n_same_reg)
    print("[DATA-INFO] Numero de categoryID duplicados :",n_same_categ)
    print("[DATA-INFO] Numero de parentCategoryID duplicados :",n_same_parent_categ)
    print("[DATA-INFO] Informacoes de diferenca de preco entre pares :",n_price_diff)
    print("[DATA-INFO] Informacoes de similaridade de attrJSON :",n_attr_sim)
    print("[DATA-INFO] Numero total de exemplos :",len(y))
    return merged_data,y

def read_merged_train_data(filepath):
    with open(filepath) as f:
        reader = csv.reader(f,delimiter=",")
        X = []
        y = []
        for line in reader:
            features = []
            for i in range(len(line)-1):
                features.append(int(line[i]))
            X.append(features)
            y.append(int(line[-1]))
    return X,y

def read_merged_test_data(filepath):
     with open(filepath) as f:
        reader = csv.reader(f,delimiter=",")
        X = []
        ids = []
        for line in reader:
            features = []
            ids.append(line[0])
            for i in range(1,len(line)):
                features.append(int(line[i]))
            X.append(features)
        return X,ids

def transform_data_non_discrete_features(items,pairs,locations,categories,file_output=False,outfile="merged_non_discrete_data.csv"):
    if file_output:
        output = open(outfile,"w")
    merged_data = []
    y = []
    n_same_metro = 0
    n_same_loc = 0
    n_same_reg = 0
    n_same_categ = 0
    n_same_parent_categ = 0
    n_price_diff = {}
    n_attr_sim = {}
    for i in range(len(pairs["itemID_1"])) :
        if "isDuplicate" in pairs:
            y.append(pairs["isDuplicate"][i])
        
        #Cria linha que sera adicionada a matriz de dados
        line = []
        
        #Adiciona o ID do individuo de teste aos dados na primeira posicao
        if "id" in pairs:
            line.append(str(pairs["id"][i]))
       
        #Carrega os dois itens para variaveis locais
        item1 = items[str(pairs["itemID_1"][i])]
        item2 = items[str(pairs["itemID_2"][i])]
        
        #calcula a distancia de jaro-winkler para os titulos dos anuncios
        #print (round(jf.jaro_winkler(item1["title"],item2["title"] * 1000)))

        #Calcula campo booleano que diz se os itens sao de mesma categoria
        if item1["categoryID"] == item2["categoryID"] :
            same_category = "1"
            n_same_categ += 1
        else :
            same_category = "0"

        line.append(same_category)
        
        #Calcula campo booleano que diz se a categoria dos itens faz parte da mesma subcategoria
        if categories[str(item1["categoryID"])]["parentCategoryID"] == categories[str(item2["categoryID"])]["parentCategoryID"] :
            same_parent_category = "1"
            n_same_parent_categ +=1
        else :
            same_parent_category = "0"
        
        line.append(same_parent_category)
        
        #Calcula campo que diz qual a diferenca entre o preco de 2 itens anunciados
        if not max(float(item1["price"]),float(item2["price"])) == 0:
            price_diff_percent = abs(float(item1["price"]) - float(item2["price"]))/max(float(item1["price"]),float(item2["price"]))
        else:
            price_diff_percent = 0
        
        line.append(price_diff_percent)
        
        #Calcula campo booleano que diz se o location e o mesmo
        if item1["locationID"] == item2["locationID"] :
            same_location = 1
            n_same_loc += 1
        else :
            same_location = 0
        
        line.append(same_location)
        
        #Calcula campo booleano que diz se a regiao do location e o mesmo
        if locations[item1["locationID"]]["regionID"] == locations[item2["locationID"]]["regionID"] :
            same_region = 1
            n_same_reg += 1
        else :
            same_region = 0
        
        line.append(str(same_region))
        
        #Calcula campo booleano que diz se o metro (seja la o que isso for) e o mesmo
        if item1["metroID"] == item2["metroID"] :
            same_metro = 1
            n_same_metro += 1
        else :
            same_metro = 0
        merged_data.append(line)
        line.append(str(same_metro))
        
        #Calculando semelhanca entre attrsJSON
        n_attrs = float(len(item1["attrsJSON"]))
        common_attrs = 0.0
        for attr in item1["attrsJSON"]:
            if attr in item2["attrsJSON"]:
                if item1["attrsJSON"][attr] == item2["attrsJSON"][attr]:
                    common_attrs += 1
        attrs_similarity = ((common_attrs+1.0)/(n_attrs+1.0))*100.0
        if not str(attrs_similarity) in n_attr_sim:
            n_attr_sim[str(attrs_similarity)] = 1
        else:
            n_attr_sim[str(attrs_similarity)] += 1
        line.append(str(attrs_similarity))
        
        if file_output:
            if "isDuplicate" in pairs:
                line.append(str(pairs["isDuplicate"][i]))
            csv_line = ",".join(line)
            csv_line += "\n"
            output.write(csv_line)
        merged_data.append(line)
    if file_output:
        output.close()
    return merged_data,y

def randomly_separate_train_data(X,y,train_prop=0.8,balance=0.5):
    cut_point = len(y) * train_prop
    positive_qty = 0
    negative_qty = 0
    total_ex  = len(y)
    X_train   = []
    y_train   = []
    X_test    = []
    y_test    = []
    j = 0
    miss = False
    while len(X_train) < cut_point:
        if not miss:
            i = np.random.randint(0,len(X))
        else:
            i = (i + 1 )% len(X)
        value = y[i]
        if j % 1000 == 0 :
            print(j)
        if value == 1:
            if positive_qty < cut_point * balance:
                X_train.append(X[i])
                y_train.append(y[i])
                del X[i]
                del y[i]
                positive_qty += 1
                j+= 1
                miss = False
            else:
                miss = True
        elif value == 0 : 
            if negative_qty < cut_point - cut_point * balance:
                X_train.append(X[i])
                y_train.append(y[i])
                del X[i]
                del y[i]
                negative_qty += 1
                j += 1
                miss = False
            else :
                miss = True
    return X_train, y_train, X, y
