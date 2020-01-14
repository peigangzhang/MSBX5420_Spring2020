from __future__ import print_function

from pyspark import SparkContext

sc = SparkContext("local", "PythonWordCount")
input_data_path = './big_data_intro.txt'
text_file = sc.textFile(input_data_path)
counts = text_file.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

print(counts.collect())
sc.stop()
