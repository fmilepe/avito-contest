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
	arq2 = "v01_neural_network.csv"

	values1 = lerCSV(arq1)
	values2 = lerCSV(arq2)

	f = open("ensemble_result.csv",'w')
	f.write("id,probability\n")

	for i in range(len(values1)):
		result = (float(values1[i])*4.0 + float(values2[i])*1.0)/5.0
		f.write(str(i)+","+str(result)+"\n")
		

ensemble()