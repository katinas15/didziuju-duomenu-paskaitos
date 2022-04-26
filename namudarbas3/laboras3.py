from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('MyFirstStandaloneApp')
sc = SparkContext(conf=conf)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Laboras3').getOrCreate()


from pyspark.sql.functions import udf, log
from pyspark.sql.types import *

text_file = spark.sparkContext.textFile("duom_cut.txt")

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


fmap = text_file.flatMap(parsinam)

aggregated_rdd = fmap.map(parsinam2) \
	.reduceByKey(lambda a, b: (a[0] + b[0], a[1]))\
	.filter(lambda l : l[1][1] == 'van').map(lambda t: (t[0], t[1][0]))

print(aggregated_rdd.take(5))


tipai = fmap.map(parsinam2).map(lambda t:(t[1][1],0))\
	.reduceByKey(lambda a,b : a).map(lambda t:t[0])
print(tipai.collect())

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType






routes = spark.read.option("header",True).csv("RouteSummary.txt")
routes.printSchema()
routes = routes.drop("M", "BendrasAtstumas","BendrasLaikas","BendrasSvoris")

def makeID(str1, str2):
    return str1+"_"+str2
	
makeID_UDF = udf(lambda z1,z2: makeID(z1,z2),StringType())

routes2 = routes.withColumn('ID', makeID_UDF("marsrutas", "sustojimo data")).drop("marsrutas", "sustojimo data")




routes = aggregated_rdd.toDF(["ID", "svoris"])
routes.show()
routes2 = routes2.withColumn("BendraKaina",routes2.BendraKaina.cast('float'))
training = routes.join(routes2, 'ID', "outer")
training = training.drop("ID")
training.show()
print(training)




from pyspark.ml.feature import VectorAssembler

vectorAssembler = VectorAssembler(inputCols = ['svoris'], outputCol = 'features')
vhouse_df = vectorAssembler.transform(training)
vhouse_df = vhouse_df.select(['features', 'BendraKaina'])
vhouse_df.show(3)




#regression ...

from pyspark.ml.regression import LinearRegression

# Load training data
#training = spark.read.format("libsvm")\
#    .load("sample_linear_regression_data.txt")


lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, featuresCol = "features", labelCol = "BendraKaina")

# Fit the model
lrModel = lr.fit(vhouse_df)

# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lrModel.coefficients))
print("Intercept: %s" % str(lrModel.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)




#dfFromRDD1.show()
#training = training.withColumnRenamed('marsrutas', 'parametrai')


training.printSchema()

pandasDF = training.toPandas()
pandasDF.head()

labels = pandasDF['_1'].to_list()

values = pandasDF['_2'].to_list()
print(labels)
print(values)

import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.ticker import LinearLocator
from mpl_toolkits.mplot3d import Axes3D

fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(20,5))
axes[2].scatter(labels,labels, s = 10)

#training.plot.scatter(x='label',
#                      y='label',
#                      c='ats',colormap='viridis',ax=axes[0])
plt.show()