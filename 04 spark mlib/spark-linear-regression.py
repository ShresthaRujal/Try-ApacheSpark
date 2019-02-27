from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":
    spark= SparkSession.builder.appName("LinearRegression").getOrCreate()
    
    inputLines = spark.sparkContext.textFile("regression.txt")
    data = inputLines.map(lambda x:x.split(",")).map(lambda x:(float(x[0]),Vectors.dense(float(x[1]))))

    #constructing dataframes of "label" and features
    colNames = ["label","features"]
    df = data.toDF(colNames)

    #spliting our data into two sets 
    trainTest = df.randomSplit([0.5,0.5])
    traingDF = trainTest[1]
    testDF= trainTest[0]

    #creating linear regression model
    linearReg = LinearRegression(maxIter=10,regParam = 0.3,elasticNetParam=0.8)
    #training the model with traningDF data
    model = linearReg.fit(traingDF)

    #using above model, predicting our testDF data
    fullPredictions = model.transform(testDF).cache()

    predictions = fullPredictions.select("prediction").rdd.map(lambda x:x[0])
    labels = fullPredictions.select("label").rdd.map(lambda x:x[0])

    predictionAndLabel = predictions.zip(labels).collect()

    for prediction in predictionAndLabel:
        print(prediction)

