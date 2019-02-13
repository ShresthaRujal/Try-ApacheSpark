import re
from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalize(text):
    #regular expression that break up text based on WORDs
    return re.compile(r'\W+',re.UNICODE).split(text.lower())

input = sc.textFile("word-count.txt")
words=input.flatMap(normalize)
# wordCounts = words.countByValue()

wordCounts = words.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
wordCountsSorted = wordCounts.map(lambda xy:(xy[1],xy[0])).sortByKey()

results =wordCountsSorted.collect()

# for word, count in wordCounts.items():
#     cleanWord = word.encode('ascii','ignore')
#     if(cleanWord):
#         print(cleanWord, count)

for result in results:
    count = str(result[0])
    word =  str(result[1].encode('ascii','ignore'))
    if word:
        print(word+":\t\t"+ count)