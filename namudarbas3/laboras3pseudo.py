lines = sc.textFile()
def fm(line):
	return line.split()

def mapF(line):
	objs = line.split()
	for at in objs:
		if(key == 'marsrutas'):
			marsrutas = val
		if(key == 'sustojimo data'):
			data = val
		if(key == 'Masinos tipas'):
			tipas = val
		if(key == 'svoris'):
			svoris = val
	return (marsrutas + "_" + data, (svoris, tipas))
  
fmap = lines.flatMap(fm)

mmap = fmap.map(mapF)\
    .filter(lambda l : l[1][1] == tipas) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1]))\
    .map(lambda t: (t[0], t[1][0]))

tipai = fmap.map(parsinam2)\
    .map(lambda t:(t[1][1],0))\
	.reduceByKey(lambda a,b : a)\
    .map(lambda t:t[0])





routes = spark.read.csv("RouteSummary.txt")
routes = routes.drop("M", "BendrasAtstumas","BendrasLaikas","BendrasSvoris")
routes2 = routes.withColumn('ID', makeID("marsrutas", "sustojimo data"))\
    .drop("marsrutas", "sustojimo data")



routes = aggregated_rdd.toDF(["ID", "svoris"])
routes.show()
routes2 = routes2.withColumn("BendraKaina",routes2.BendraKaina.cast('float'))
training = routes.join(routes2, 'ID', "outer")
training = training.na.drop("any")



vectorAssembler = VectorAssembler(inputCols = ['svoris'], outputCol = 'features')
vhouse_df = vectorAssembler.transform(training)
vhouse_df = vhouse_df.select(['features', 'BendraKaina'])
vhouse_df.show(3)


lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, 
        featuresCol = "features", labelCol = "BendraKaina")
lrModel = lr.fit(vhouse_df)


trainingSummary = lrModel.summary
a=lrModel.intercept
b=lrModel.coefficients[0]
print(f'a={a}  b={b}')
print("r2: %f" % trainingSummary.r2)

fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(20,5))
axes[2].scatter(x,y, s = 10)
lineX = [min(x), max(x)]
lineY = [a+b*lineX[0],a+b*lineX[1]]
axes[2].plot(lineX, lineY)
plt.show()
