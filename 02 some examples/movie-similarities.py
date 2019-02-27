import sys
from pyspark import SparkConf, SparkContext
from math import sqrt


# 'local[*]' using spark built-in cluster manager and treat every core of computer as a node on a cluster
conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf =conf)

def loadMovieNames():
    movieNames ={}
    with open("u.item", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def makePairs(userRatings):
    ratings = userRatings[1]
    (movie1,rating1) = ratings[0]
    (movie2,rating2) = ratings[1]
    return ((movie1,movie2),(rating1,rating2))

def filterDuplicates(userRatings):
    ratings = userRatings[1]
    (movie1,rating1) = ratings[0]
    (movie2,rating2) = ratings[1]
    # enforcing to return only if movie1 is smaller than movie2
    return movie1 < movie2

def computeCosineSimilarity(ratingParis):
    numPairs = 0 
    sum_xx = sum_yy = sum_xy = 0
    for ratingX,ratingY in ratingParis:
        sum_xx +=ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if(denominator):
        score = (numerator / float(denominator))
        return (score, numPairs)


print("\n Loading Moive names...")
nameDict = loadMovieNames()

data = sc.textFile("u.data")
# maps line into (user,(movieId,rating))
ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

# convert above map into (user,((movieId1,rating1),(movieId2,rating2))
# example :
# (a,(b0,c0)),(a,(b1,c1)),(a,(b2,c2)) into => (a,((b0,c0),(b1,c1))) also (a,((b0,c0),(b2,c2))) 
# also (a,((b1,c1),(b0,c0))) also (a,((b1,c1),(b1,c1))) also (a,((b1,c1),(b2,c2))) and soon
joinedRatings = ratings.join(ratings)
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

moviePairs = uniqueJoinedRatings.map(makePairs)

# gathers up the rating pairs according to their unique keys
moviePairRatings = moviePairs.groupByKey()

moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    movieId = int(sys.argv[1])

    # filter out the movie pairs containing movieId which we are intrested in and minimum
    # threshold in the qualitiy of similarity and also viewed by more than 50 people
    filteredResults = moviePairSimilarities.filter(lambda pairsim: \
        (pairsim[0][0] == movieId or pairsim[0][1] == movieId) \
            and pairsim[1][0] > scoreThreshold and pairsim[1][1] > coOccurenceThreshold)
    
    
    results = filteredResults.map(lambda pairsim: (pairsim[1],pairsim[0])).sortByKey(ascending = False).take(10)
    
    print("Top 10 similar movies for " + nameDict[movieId])
    for result in results:
        (sim,pair) = result
        similarMovieId = pair[1]
        if(similarMovieId == movieId):
            similarMovieId == pair[1]
        print(nameDict[similarMovieId] + "\tscore: " + str(sim[0]) + "\t strength: "+str(sim[1]))
