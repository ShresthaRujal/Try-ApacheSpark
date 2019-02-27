from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext
from graphframes import *
import collections


def mapper(line):
    fields = line.split()
    return Row(userId=int(fields[0]),movieId=int(fields[1]),rating=int(fields[2]),timestamp=int(fields[3]))

spark = SparkSession.builder.appName("movieGraph").getOrCreate()
movie = lines.map(mapper)

schemaMovie = spark.createDataFrame(movie).cache()
schemaMovie.createOrReplaceTempView("movie")

movieWatchedByUser = spark.sql("SELECT userId,movieId from movie")
movieRatings = spark.sql("SELECT movieId,rating from movie")

g= GraphFrame(movieWatchedByUser,movieRatings)

print(g)


spark.stop()
