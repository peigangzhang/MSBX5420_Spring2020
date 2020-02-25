from __future__ import print_function

from pyspark.sql import SparkSession
import pydoop.hdfs as pyhdfs
import os
import sys


def copy_file(src_path, dst_path):
    """
    copy one path
    :param src_path:
    :param dst_path:
    """
    if not pyhdfs.path.exists(dst_path):
        pyhdfs.cp(src_path, dst_path)


def main():
    src_dir = str(sys.argv[1])
    dst_dir = str(sys.argv[2])

    # create dst_dir if not exist
    if not pyhdfs.path.exists(dst_dir):
        pyhdfs.mkdir(dst_dir)

    # create sparkcontext
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # create children path rdd
    children_paths = pyhdfs.ls(src_dir)
    children_paths_rdd = sc.parallelize(children_paths)

    # each executor task is to copy one children path
    children_paths_rdd.foreach(
        lambda file_path: copy_file(file_path, os.path.join(dst_dir, os.path.basename(file_path))))

    # stop sparkcontext
    sc.stop()


if __name__ == "__main__":
    """
    To run on Yarn:
    spark-submit --master yarn --deploy-mode cluster --num-executors 2 --executor-memory 1G --executor-cores 1 --driver-memory 1G ./spark_example_hdfs_copy.py hdfs://msbx5420-m/user/peter hdfs://msbx5420-m/test

    To run on local mode:
    spark-submit --master local[2] ./spark_example_hdfs_copy.py hdfs://msbx5420-m/user/peter hdfs://msbx5420-m/test
    """
    main()
