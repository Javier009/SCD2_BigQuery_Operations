import random
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import  *

from spark_builder import *
from read_data import *
from pydeequ_checks import *
from writer import *

from google.cloud import bigquery
from google.cloud import storage


PROJECT_ID = "dataproc-spark-461405"
BUCKET_NAME = 'transactions_raw_data'
ARCHIVE_BUCKET = 'transactions_raw_data_archive'
GCS_client = storage.Client()

# BigQuery variables
DATA_SET = 'financial_transactions'
TRANSACTIONS_TABLE =  'daily_transactions'
CUSTOMER_INFO_STAGING_TABLE = 'stg_daily_customer_info'
CUSTOMER_INFO_TARGET_TABLE = 'customer_profile_scd2'
bq_client = bigquery.Client()

def main():
    # 1 --> Create a Spark Session and fetch dates from raw data bucket---
    spark = create_spark_session()
    dates = list_gcs_files_dates(BUCKET_NAME, GCS_client)

    if dates:
        for date in dates:

            local_test_path =  f"/Users/delgadonoriega/Desktop/gcp-data-eng-bootcamp/Module_3_class_1/pyspark_src/transactions_{date}.json"  # Pending code to build GCS path
            # gcs_path =f"gs://{BUCKET_NAME}/transactions_{date}.json"
            # gcs_path_archive = f"gs://{ARCHIVE_BUCKET}/transactions_{date}.json"

            # 2 --> Read the data from GCS as PySpark df ---
            data = read_data(spark , local_test_path)

            transactions = data.withColumn("transaction_date", date_format("transaction_timestamp", "yyyy-MM-dd")) \
                            .select("transaction_id", "transaction_timestamp", 'transaction_date', "amount", "currency", "merchant_id", "merchant_category","card_type", "transaction_status", "customer_info.customer_id") \
                                    
            customer_info = data.select("customer_info.*") \
                                .withColumn("effective_start_date", lit(date)) \
                                .withColumn("effective_end_date", lit('3000-12-31')) \
                                .withColumn("is_current", lit(True)) \
                                .distinct()

            # 3 --> PyDeequ Checks ---
            run_quality_checks(spark, data)

            # 4 --> Write all daily to transactions table
            # write_to_transactions_table(PROJECT_ID, DATA_SET, TRANSACTIONS_TABLE, transactions)
            big_query_write_test(project_id=PROJECT_ID, dataset=DATA_SET, table=TRANSACTIONS_TABLE, BQ_client=bq_client, df=transactions, overwrite=False)
            
            # 5 --> Write to stagin table table
            # write_to_transactions_table(PROJECT_ID, DATA_SET, CUSTOMER_INFO_STAGING_TABLE, customer_info)
            big_query_write_test(project_id=PROJECT_ID, dataset=DATA_SET, table=CUSTOMER_INFO_STAGING_TABLE, BQ_client=bq_client, df=customer_info, overwrite=True)

            # 6 --> SCD2 with target table
            merge_scd2_bq(project_id=PROJECT_ID, dataset=DATA_SET, staging_table = CUSTOMER_INFO_STAGING_TABLE, target_table =CUSTOMER_INFO_TARGET_TABLE , BQ_client = bq_client)

            # 7 --> Move processed file to Archive Folder in GCS
            move_json_between_buckets(GCS_client = GCS_client , src_bucket_name = BUCKET_NAME,  dst_bucket_name = ARCHIVE_BUCKET, date = date)
        # Finish Spark Session
    else:
        print('No new files to process ... ')
    close_spark_session(spark)
    os._exit(0)  # Since pydeqeq bug not finishing the session we force it
    

if __name__ == '__main__':
    main()