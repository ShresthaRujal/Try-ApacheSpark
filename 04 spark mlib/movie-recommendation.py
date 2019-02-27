import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS,Rating

def loadMovieNames():
    movieNames = {}
    with open("../02 some examples/u.item",encoding='ascii', errors="ignore") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local[*]").setAppName("MovieRecommendation")
sc = SparkContext(conf=conf)

nameDict = loadMovieNames()

data = sc.textFile("../02 some examples/u.data")
#formatting data to feed into ALS
#returns "Result(userId,movieId,rating) "
ratings = data.map(lambda l : l.split()).map(lambda l: Rating(int(l[0]),int(l[1]),float(l[2]))).cache()

print("Traning recommendation model usling Alternating least Squares")

rank=10
numIterations =20
model = ALS.train(ratings,rank,numIterations)

userId = int(sys.argv[1])
print("Ratings for user Id "+str(userId) + ":")
userRatings = ratings.filter(lambda l : l[0] == userId)
for rating in userRatings.collect():
    print(nameDict[int(rating[1])] + ": "+ str(rating[2]))

print("\nTop 10 recommendations:")
recommendations = model.recommendProducts(userId,10)
for recommendation in recommendations:
    print(nameDict[int(recommendation[1])] + "score "+str(recommendation[2]))
