from pyspark import SparkConf,SparkContext

# Boiler Plate
conf = SparkConf().setMaster("local").setAppName("MinTemperature")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID =fields[0]
    entryType=fields[2]
    temperature = float(fields[3])*1.8+32.0
    return (stationID,entryType,temperature)

lines = sc.textFile("min-temp.txt")
parseLines = lines.map(parseLine)
# Using Filter
minTemps = parseLines.filter(lambda x:"TMIN" in x[1])
stationTemps = minTemps.map(lambda x:(x[0],x[2]))
# Taking min of same key
minTemps = stationTemps.reduceByKey(lambda x,y: min(x,y))
results = minTemps.collect()

for result in results:
    print(result[0]+"\t{:.2f}F".format(result[1]))