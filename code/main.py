

import logging

from pyspark.sql import SparkSession

def create_spark_session():
    logging.info("creating spark session")
    return SparkSession.builder.appName('Quadrant Repartitioning').getOrCreate()

def read_parquet_file(sparkSession):
    logging.info("reading parquet file")
    sparkSession.read.parquet("/Users/pratikjoshi/Downloads/20220414_054724_00091_4mhk3_bucket-00001").show(10,False)
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    read_parquet_file(create_spark_session())

