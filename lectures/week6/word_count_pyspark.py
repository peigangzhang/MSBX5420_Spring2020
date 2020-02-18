from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .getOrCreate()

sc = spark.sparkContext

input_data_path = 'hdfs://msbx5420-m/user/peter/big_data_intro.txt'
text_rdd = sc.textFile(input_data_path)

counts_rdd = text_rdd.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

counts_df = counts_rdd.toDF((['word', 'count']))
counts_df.write.parquet('hdfs://msbx5420-m/user/peter/counts.parquet')

sc.stop()