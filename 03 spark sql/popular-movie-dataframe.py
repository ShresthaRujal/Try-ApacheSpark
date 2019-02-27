from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMoviesNames():
    moviesNames={}
    with open("../02 some examples/u.item",encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            moviesNames[int(fields[0])]= fields[1]
    return moviesNames


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict = loadMoviesNames()
lines = spark.sparkContext.textFile("../02 some examples/u.data")
movies = lines.map(lambda x:Row(movieId=int(x.split()[1])))
movieDataset = spark.createDataFrame(movies)

topMovieIds= movieDataset.groupBy("movieId").count().orderBy("count",ascending=False).cache()
topMovieIds.show()
top10 = topMovieIds.take(10)

for result in top10:
    print (nameDict[result[0]]+" : "+str(result[1]))

spark.stop()