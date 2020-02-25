from __future__ import print_function

import sys
from operator import add
from random import random

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import random


def get_random_int(_):
    return random.randint(0, 10)


def simulation(row):
    """
    get simulation result
    :return:
    """
    fixed_cost = 120000
    profit = row.volume * (row.price - row.cost) - fixed_cost
    return profit


def main():
    # get simulation fraction
    simulation_fraction = float(sys.argv[1]) if len(sys.argv) > 1 else 100.0

    # create sparkcontext
    spark = SparkSession.builder.getOrCreate()

    # load sales and cost dataframe
    sales_df = spark.read.json("./sales_scenarios.json")
    cost_df = spark.read.json("./sales_cost.json")

    # define a udf to create new column with random int
    udf_get_random_int = udf(get_random_int, IntegerType())

    # sample sales dataframe with random int
    sampled_sales_df = sales_df.sample(withReplacement=True, fraction=simulation_fraction)
    sampled_sales_with_randint_df = sampled_sales_df.withColumn("randint", udf_get_random_int('sales_scenarios'))

    # sample cost dataframe with random int
    sampled_cost_df = cost_df.sample(withReplacement=True, fraction=simulation_fraction)
    sampled_cost_with_randint_df = sampled_cost_df.withColumn("randint", udf_get_random_int('name'))

    # get joined dataframe
    joined_df = sampled_sales_with_randint_df.join(sampled_cost_with_randint_df,
                                                   sampled_sales_with_randint_df.randint == sampled_cost_with_randint_df.randint)

    # get total profit by map reduce with profit simulation
    total_profit = joined_df.rdd.map(simulation).reduce(add)
    total_simulations = joined_df.count()

    print('Profit is {}'.format(total_profit / total_simulations))

    spark.stop()


if __name__ == "__main__":
    """
    To run on Yarn:
    spark-submit --master yarn --deploy-mode cluster --num-executors 2 --executor-memory 1G --executor-cores 1 --driver-memory 1G ./spark_example_monte_carlo.py 2

    To run on local mode:
    spark-submit --master local[2] ./spark_example_monte_carlo.py 2
    """
    main()
