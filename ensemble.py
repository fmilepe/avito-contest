import csv

def lerCSV(path):
	values = []
	i = 0
	with open(path, 'r') as csvfile:
		datareader = csv.reader(csvfile,delimiter=',')
		for v in datareader:
			if(i > 0):
				values.append(v[1])
			i = i + 1
	return values

def ensemble():
	arq1 = "knn.csv"
	arq2 = "xgboost.csv"

	values1 = lerCSV(arq1)
	values2 = lerCSV(arq2)

	f = open("ensemble_result.csv",'w')
	f.write("id,probability\n")
	ctz = 0
	big_diff = 0
	avg_cont = 0
	for i in range(len(values1)):
		knn = float(values1[i])
		xgb = float(values2[i])
		
		if(xgb >= 0.6 or xgb <= 0.4):
			ctz = ctz + 1
			result = xgb
		else:
			if(abs(xgb-knn) >= 0.4):
				big_diff = big_diff + 1
				result = knn
			else:
				avg_cont = avg_cont + 1
				#result = (knn+xgb)/2.0
				result = xgb

		f.write(str(i)+","+str(result)+"\n")
	print("XGB ctz: ", ctz/i*100)
	print("Big Diff:", big_diff/i*100)
	print("AVG xgb & knn:", avg_cont/i*100)

ensemble()