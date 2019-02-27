from pyspark import SparkContext
from pyspark.streaming import StreamingContext


sc = SparkContext("local[*]","WordCount")
stream = StreamingContext(sc,1)
lines = stream.socketTextStream("localhost",8080)
# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))
# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
stream.start()             # Start the computation
stream.awaitTermination()  # Wait for the computation to terminate