from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("popularMOvie")
sc = SparkContext(conf=conf)

# ratings.csv contains USERID,MOVIEID,RATINGS,TIMESTAMP respectively
lines = sc.textFile("ratings.csv")
movies = lines.map(lambda x: (int(x.split(',')[1]),1))
movieCounts = movies.reduceByKey(lambda x,y:x+y)

filpped = movieCounts.map(lambda xy:(xy[1],xy[0]))
sortedMovies = filpped.sortByKey()
results = sortedMovies.collect()

for result in results:
    print(result)

