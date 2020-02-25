from pyspark.sql import SparkSession


def main():
    # create sparkcontext
    spark = SparkSession.builder.getOrCreate()

    sc = spark.sparkContext

    # read input file from hdfs
    input_data_path = 'hdfs://msbx5420-m/user/peter/big_data_intro.txt'
    text_rdd = sc.textFile(input_data_path)

    # counts rdd
    counts_rdd = text_rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(
        lambda a, b: a + b)

    # save intermedidate rdd
    counts_rdd.saveAsTextFile('hdfs://msbx5420-m/user/peter/counts_rdd')

    # create counts dataframe
    counts_df = counts_rdd.toDF((['word', 'count']))

    # write counts dataframe to hdfs
    counts_df.write.parquet('hdfs://msbx5420-m/user/peter/counts.parquet')

    # stop sparkcontext
    sc.stop()


if __name__ == "__main__":
    main()
