
from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreeOfSeparation")
sc = SparkContext(conf=conf)

startCharacterID = 5306
targetCharacterId = 14

# signal when find the target character during traversal
hitCounter = sc.accumulator(0)

def convertToBFS(line):
    fields = line.split()
    heroId =int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))
    
    color = 'WHITE'
    distance = 9999
    if(heroId == startCharacterID):
        color = "GRAY"
        distance=0
    
    return (heroId,(connections,distance,color))

def createStartingRdd():
    inputFile =  sc.textFile("marvel-graph.txt")
    return inputFile.map(convertToBFS)

def bfsMap(node):
    #creats new nodes for each connection for gray nodes with a distance incremented by one
    #color gray and no connections also color the gray node with black after just processed.
    characterId = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if(color=='GRAY'):
        for connection in connections:
            newCharacterId = connection
            newDistance = distance +1
            newColor = "GRAY"
            if(targetCharacterId == connection):
                hitCounter.add(1)

            newEntry = (newCharacterId,([],newDistance,newColor))
            results.append(newEntry)

        color = "BLACK"
    results.append((characterId,(connections,distance,color)))
    
    return results

def bfsReduce(data1,data2):
    
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]
    distance = 9999
    color = "WHITE"
    edges = []

    if(len(edges1)>0):
        edges = edges1
    elif (len(edges2)>0):
        edges = edges2

    if(distance1<distance):
        distance = distance1
    
    if(distance2 < distance):
        distance = distance2
    
    if(color1=='WHITE' and (color2=='GRAY' or color2=='BLACK')):
        color = color2
    
    if(color2 == 'GRAY' and color2 =='BLACK'):
        color = color2
    return (edges,distance,color)

iterationRdd = createStartingRdd()

for iteration in range(0,10):
    print("Running BFS interation "+ str(iteration+1))

    mapped = iterationRdd.flatMap(bfsMap)
    print("processing "+str(mapped.count()) + " values.")
    if(hitCounter.value>0):
        print("Hit the target Character! From " + str(hitCounter.value)+ " different direction(s).")
        break

    iterationRdd = mapped.reduceByKey(bfsReduce)
    

