# import random
import re
# from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import  *


def list_gcs_files_dates(bucket_name, GCS_client, prefix=None):

    file_names_dates = []
    blobs = GCS_client.list_blobs(bucket_name, prefix=prefix)

    print(f"Listing files in bucket: '{bucket_name}' (prefix: '{prefix if prefix else 'None'}')")
    for blob in blobs:
        match = re.search(r"transactions_(\d{4}-\d{2}-\d{2})\.json", blob.name)
        if match:    
            date_str = match.group(1)
            file_names_dates.append(date_str)
        else:
            pass
    return file_names_dates

def read_data(spark, gcs_path):
    try:
        df = spark.read.json(gcs_path)
        print(f"Successfully read JSON from: {gcs_path}")
        # df.printSchema()
        # df.show(5, truncate=False)
        return df

    except Exception as e:
        print(f"Error reading JSON from GCS: {e}")
    



