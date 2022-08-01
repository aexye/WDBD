from pyspark import SparkConf, SparkContext

# oryginalna nazwa skryptu: book_words_count.py

conf = SparkConf().setMaster("local").setAppName("BookWordsCount")
sc = SparkContext(conf=conf)

lines = sc.textFile("pg20417.txt")

words = lines.flatMap(lambda x: x.split(" "))
pairs = words.map(lambda x: (x, 1))
wordCount = pairs.reduceByKey(lambda a, b: a + b)

results = wordCount.collect()

for result in results:
    # if result[1] > 5: # dla przejrzystości wyników
    print("{}\t{}".format(result[0], result[1]))
