from __future__ import print_function

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


def simulation(_):
    """
    get simulation result
    :return:
    """
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0


def main():
    # create sparkcontext
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # get partitions from input argument
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    # each partition simulate 100000 times
    n = 100000 * partitions

    count = spark.sparkContext.parallelize(range(n), partitions).map(simulation).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n))

    # stop sparkcontext
    sc.stop()


if __name__ == "__main__":
    """
    To run on Yarn:
    spark-submit --master yarn --deploy-mode cluster --num-executors 2 --executor-memory 1G --executor-cores 1 --driver-memory 1G ./spark_example_monte_carlo.py 2

    To run on local mode:
    spark-submit --master local[2] ./spark_example_monte_carlo.py 2
    """
    main()
