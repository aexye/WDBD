from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    temperature = float(fields[3])
    return (temperature)

#umiescic plik w odpowiednim folderze w hdfs
lines = sc.textFile("hdfs://localhost:9000/temp.csv")
parsedLines = lines.map(parseLine)
stationTemps = parsedLines.map(lambda x: (x,1))
countTemps = stationTemps.reduceByKey(lambda a, b: a + b)
results = countTemps.collect();

for result in results:
    print("{}\t{}".format(result[0], result[1]))

