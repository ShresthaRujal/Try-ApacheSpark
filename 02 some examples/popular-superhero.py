from pyspark import SparkConf,SparkContext

conf =  SparkConf().setMaster("local").setAppName("popularSuperHero")
sc = SparkContext(conf=conf)

def countCoOccurence(line):
    elements = line.split()
    return (int(elements[0]),len(elements)-1)

def parseName(line):
    fields= line.split('\"')
    return (int(fields[0]),fields[1].encode("utf8"))

names = sc.textFile("marvel-names.txt")
namesRdd = names.map(parseName)

lines = sc.textFile("marvel-graph.txt")
parings = lines.map(countCoOccurence)
totalFriendsByCharacter = parings.reduceByKey(lambda x,y:x+y)
flipped= totalFriendsByCharacter.map(lambda xy:(xy[1],xy[0]))

mostPopular = flipped.max()
mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(str(mostPopularName) + " is the most popular super hero with " + \
    str(mostPopular[0]) + " co-apperances.")