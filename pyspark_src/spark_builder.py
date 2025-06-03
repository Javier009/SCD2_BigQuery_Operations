from pyspark.sql import SparkSession
import os

os.environ["SPARK_VERSION"] = "3.5"

def create_spark_session(environemnt = 'Remote'):
    if environemnt == 'Remote':
        spark = SparkSession.builder \
                .appName(f"Read_GCS_Transactions_data") \
                .config(
                "spark.jars.packages",
                ",".join([
                    "com.amazon.deequ:deequ:2.0.11-spark-3.5"
                ])
                )\
                .getOrCreate()
    else:
        spark = SparkSession.builder \
                .appName("Append data to BigQuery Table") \
                .config(
                    "spark.jars.packages",
                    ",".join([
                        "com.google.cloud.hadoop:gcs-connector:hadoop2-2.1.1",                  
                        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0",                        
                        "com.amazon.deequ:deequ:2.0.11-spark-3.5"
                    ])
                ) \
                .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "false") \
                .config("spark.cloud.google.temp.gcs.bucket", "spark-staging-bucket-transactions") \
                .getOrCreate()

    return spark

def close_spark_session(spark_object):
    spark_object.stop()