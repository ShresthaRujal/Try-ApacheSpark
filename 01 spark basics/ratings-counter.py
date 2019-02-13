from pyspark import SparkContext,SparkConf
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)
lines = sc.textFile("ratings-counter.txt")
ratings= lines.map(lambda x:x.split()[2])
result = ratings.countByValue()
sortedResults = collections.OrderedDict(sorted(result.items()))
for key,value in sortedResults.items():
    print(key, value)