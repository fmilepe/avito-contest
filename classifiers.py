import time
import numpy as np
import sys
from sklearn.neural_network import MLPClassifier
from sklearn import cross_validation
from sklearn.preprocessing import StandardScaler
from sklearn.cross_validation import KFold
from sklearn.preprocessing import normalize
import random
from sklearn.neighbors import KNeighborsClassifier
from mongo_database import *
import xgboost as xgb
from feature_creator import generateClusterFeature

def carregarFeaturesKaggle(limit, collection_name):
	print("Carregando features do Kaggle")
	client = openMongoConnection()
	db = client[DB_NAME]
	if limit == -1:
		cursor = db[collection_name].find({})
	else:
		cursor = db[collection_name].find({}).limit(limit)
	
	X = []
	for doc in cursor:
		X.append(doc['x'])

	X = np.array(X)
	client.close()

	return X

def carregarFeaturesTreino(limit, collection_name):
	print("Carregando features de treino")
	client = openMongoConnection()
	db = client[DB_NAME]

	if limit == -1:
		cursor = db[collection_name].find({})
	else:
		cursor = db[collection_name].find({}).limit(limit)
	X = []
	y = []
	for doc in cursor:
		X.append(doc['x'])
		y.append(int(doc['y']))

	c = list(zip(X,y))
	random.shuffle(c)
	X,y = zip(*c)
	X = np.array(X)
	y = np.array(y)
	client.close()

	return [X, y]

def rede_neural(X, y):
	print("Iniciando treinamento da Rede Neural")

	X2 = normalize(X)

	clf = MLPClassifier(hidden_layer_sizes=(100,50), activation='tanh', algorithm='adam', alpha=1e-5,
						learning_rate='constant',tol=1e-8,learning_rate_init=0.0002,
						early_stopping=True,validation_fraction=0.2)

	kf = KFold(len(y),n_folds=3)
	i = 0
	for train,test in kf:
		start = time.time()
		i = i + 1
		print("Treinamento",i)

		# dividindo dataset em treino e test
		#X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.4, random_state=1)
		X_train, X_test, y_train, y_test = X2[train], X2[test], y[train], y[test]
		
		# fit
		clf.fit(X_train, y_train)
		print("score:",clf.score(X_test, y_test),"(",(time.time()-start)/60.0,"minutos )")
	return clf

def XGBoost(X, y):
	print("Iniciando treinamento do XGBoost")
	start_time = time.time()

	X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.1, random_state=1)
	clf = xgb.XGBClassifier(learning_rate=0.15, n_estimators=170, nthread=6, max_depth=8, seed=0, silent=True,
							subsample=0.85, colsample_bytree=0.85)
	clf.fit(X, y)
	score = clf.score(X_test, y_test)
	print("XGBoost score: ", score, "(", (time.time()-start_time)/60.0, "minutos )")

	return clf

def KNN(X, y):
	print("Iniciando treinamento do KNN")
	clf = KNeighborsClassifier(n_jobs=6,leaf_size=15)
	kf = KFold(len(y),n_folds=20)
	clf.fit(X,y)

	X_score = X[:10000]
	y_score = y[:10000]
	score = clf.score(X_score, y_score)
	print("KNN score: ", score)

	return clf

def generate_kaggle_submition(X_Kaggle, clf, output):
	print("Fazendo predict do classificador")
	predict = clf.predict_proba(X_Kaggle)

	print("Criando submissao", output)
	i = 0
	f = open(output,'w')
	f.write("id,probability\n")
	for p in predict:
		f.write(str(i)+","+str(p[1])+"\n")
		i = i + 1
	f.close()

def classify(learn_rede_neural=False, learn_KNN=False, learn_xgb=False):
	if(learn_rede_neural or learn_KNN):
		X, y = carregarFeaturesTreino(-1, FEATURES_COLL)
		X_Kaggle = carregarFeaturesKaggle(-1, FEATURES_TEST_COLL)

	if(learn_xgb):
		X_disc, y_disc = carregarFeaturesTreino(-1, FEATURES_XGB_COLL)
		X_Kaggle_disc = carregarFeaturesKaggle(-1, FEATURES_XGB_TEST_COLL)
		clusterClf = generateClusterFeature(X_disc)

		cluster_x = clusterClf.predict(X_disc)
		i = 0
		for x_f in X_disc:
			np.append(x_f, cluster_x[i])
			i = i + 1

		cluster_x = clusterClf.predict(X_Kaggle_disc)
		i = 0
		for x_f in X_Kaggle_disc:
			np.append(x_f, cluster_x[i])
			i = i + 1

	if(learn_rede_neural):
		clf1 = rede_neural(X, y)
		generate_kaggle_submition(X_Kaggle, clf1, 'neural_network.csv')
	
	if(learn_KNN):
		clf2 = KNN(X, y)
		generate_kaggle_submition(X_Kaggle, clf2, 'knn.csv')

	if(learn_xgb):
		clf3 = XGBoost(X_disc, y_disc)
		generate_kaggle_submition(X_Kaggle_disc, clf3, 'xgboost.csv')
