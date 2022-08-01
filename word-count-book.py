from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("BookWordsCount")
sc = SparkContext(conf=conf)

line = sc.textFile("pg20417.txt")

words = line.flatMap(lambda x: x.split(" "))
pairs = words.map(lambda x: (x, 1))
wordCount = pairs.reduceByKey(lambda a, b: a + b)


counts = wordCount.collect()

for count in counts:
    print("{}\t{}".format(count[0], count[1]))
