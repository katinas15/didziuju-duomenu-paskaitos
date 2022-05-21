from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('MyFirstStandaloneApp')
sc = SparkContext(conf=conf)







from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Laboras3').getOrCreate()

from pyspark.sql.functions import udf, log
from pyspark.sql.types import *


# vyksta siuntimas tarp serverio mazgu ir lokalaus kompiuter
text_file = spark.sparkContext.textFile("duom_full.txt")    

def parsinam(line):
	return line[2:len(line)-2].split('}}{{')

def parsinam2(line):
	objs = line.split('}{')
	k1=None
	k3=None
	tipas= None
	svoris=None
	for at in objs:
		temp = at.split('=')
		if(len(temp)<2): 
			break
		key,val=at.split('=')
		if(key == 'marsrutas'):
			k1=val
		if(key == 'sustojimo data'):
			k3=val
		if(key == 'Masinos tipas'):
			tipas=val
		if(key == 'svoris'):
			svoris=val
	if(k1!=None and k3!=None and tipas!=None and svoris!=None):
		return (k1+"_"+k3, (float(svoris), tipas))
	else:
		return ("0", (1, "blogai"))
  

#vyksta lokaliai
fmap = text_file.flatMap(parsinam)
#vyksta lokaliai
tipai = fmap.map(parsinam2).map(lambda t:(t[1][1],0))\
	.reduceByKey(lambda a,b : a).map(lambda t:t[0])








from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# vyksta siuntimas tarp serverio mazgu ir lokalaus kompiuter
routes = spark.read.option("header",True).csv("RouteSummary.txt", inferSchema=True)
routes.printSchema()
routes = routes.drop("M", "BendrasAtstumas","BendrasLaikas","BendrasSvoris")

def makeID(str1, str2):
    return str(str1)+"_"+str2

makeID_UDF = udf(lambda z1,z2: makeID(z1,z2),StringType())
#vyksta lokaliai
routes2 = routes.withColumn('ID', makeID_UDF("marsrutas", "sustojimo data")).drop("marsrutas", "sustojimo data")








from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.ticker import LinearLocator
from mpl_toolkits.mplot3d import Axes3D

for tipas in tipai.collect():
    if tipas == "blogai":
        continue

    print(tipas)
    print(tipas)
    print(tipas)
    print(tipas)
    print(tipas)

    #vyksta lokaliai
    aggregated_rdd = fmap.map(parsinam2) \
        #vyksta lokaliai
        .filter(lambda l : l[1][1] == tipas) \
        # vyksta siuntimas tarp serverio mazgu ir lokalaus kompiuter
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1]))\
        #vyksta lokaliai
        .map(lambda t: (t[0], t[1][0]))

    # vyksta siuntimas tarp serverio mazgu ir lokalaus kompiuter
    routes = aggregated_rdd.toDF(["ID", "svoris"])
    #vyksta lokaliai
    routes2 = routes2.withColumn("BendraKaina",routes2.BendraKaina.cast('float'))
    # vyksta siuntimas tarp serverio mazgu ir lokalaus kompiuter
    training = routes.join(routes2, 'ID', "outer")
    #vyksta lokaliai
    training = training.na.drop("any")


    # vyksta siuntimas tarp serverio mazgu ir lokalaus kompiuter
    vectorAssembler = VectorAssembler(inputCols = ['svoris'], outputCol = 'features')
    vhouse_df = vectorAssembler.transform(training)
    vhouse_df = vhouse_df.select(['features', 'BendraKaina'])


    lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, featuresCol = "features", labelCol = "BendraKaina")

     # vyksta siuntimas tarp serverio mazgu ir lokalaus kompiuter
    lrModel = lr.fit(vhouse_df)

    #viskas kitas vyksta lokaliai
    trainingSummary = lrModel.summary
    a=lrModel.intercept
    b=lrModel.coefficients[0]
    print(f'masinos tipas - {tipas}')
    print(f'a={a}  b={b}')
    print("r2: %f" % trainingSummary.r2)


    pandasDF = training.toPandas()
    x = pandasDF['svoris'].to_list()
    y = pandasDF['BendraKaina'].to_list()



    fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(5,15))
    axes[0].scatter(x,y, s = 10)
    lineX = [min(x), max(x)]
    lineY = [a+b*lineX[0],a+b*lineX[1]]
    axes[0].plot(lineX, lineY)
    plt.show()