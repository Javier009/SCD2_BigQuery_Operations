from pyspark.sql import SparkSession
import os

os.environ["SPARK_VERSION"] = "3.5"

def create_spark_session(environemnt = 'Remote'):
    spark = SparkSession.builder \
                .appName(f"Read_GCS_Transactions_data") \
                .config(
                "spark.jars.packages",
                ",".join([
                    "com.amazon.deequ:deequ:2.0.11-spark-3.5"
                ])
                )\
                .getOrCreate()
    return spark

def close_spark_session(spark_object):
    spark_object.stop()