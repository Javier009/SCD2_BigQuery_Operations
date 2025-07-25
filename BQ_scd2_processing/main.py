import pandas as pd
from datetime import datetime

from utils.read_data import *
from utils.checks import *
from utils.wrtier import *
from utils.scd2 import *

from google.cloud import bigquery
from google.cloud import storage
import functions_framework
import gcsfs


PROJECT_ID = "dataproc-spark-461405"
BUCKET_NAME = 'transactions_raw_data'
ARCHIVE_BUCKET = 'transactions_raw_data_archive'

# BigQuery variables
DATA_SET = 'financial_transactions'
TRANSACTIONS_TABLE =  'daily_transactions'
CUSTOMER_INFO_STAGING_TABLE = 'stg_daily_customer_info'
CUSTOMER_INFO_TARGET_TABLE = 'customer_profile_scd2'


@functions_framework.cloud_event
def main(cloud_event):

    GCS_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    fs = gcsfs.GCSFileSystem(project='dataproc-spark-461405')

    # 1 --> Create a Spark Session and fetch dates from raw data bucket---
    dates = list_gcs_files_dates(BUCKET_NAME, GCS_client)
    print(dates)
    if dates:
        for date in dates:    

            gcs_path =f"gs://{BUCKET_NAME}/transactions_{date}.json"
            print(f'Processing {gcs_path}')
            # gcs_path_archive = f"gs://{ARCHIVE_BUCKET}/transactions_{date}.json"

            # 2 --> Read the data from GCS as Pandas Data Frame df ---

            # All data modifications
            data_all = read_data(fs , gcs_path)            

            data_all['transaction_timestamp'] = data_all['transaction_timestamp'].apply(lambda ts: datetime.strptime(ts, '%Y-%m-%dT%H:%M:%SZ'))
            data_all['transaction_date'] = date
            data_all['transaction_date'] = pd.to_datetime(data_all['transaction_date'], errors='coerce')
            data_all['transaction_timestamp'] =  data_all['transaction_timestamp'].apply(lambda ts: str(ts))            
            data_all['amount'] = data_all['amount'].astype(str)                 
            data_all['customer_id'] = data_all['customer_info'].apply(lambda x: x['customer_id'])

            # Daily transactions data
            transactions = data_all[["transaction_id", 'transaction_date',  "currency", "merchant_id", "merchant_category","card_type", "transaction_status", "customer_id", "transaction_timestamp", "amount"]]

            # Customer dimensions table for SCD2 process            
            customer_info = pd.DataFrame([d for d in data_all['customer_info']])
            customer_info['effective_start_date'] = date
            customer_info['effective_start_date'] = pd.to_datetime(customer_info['effective_start_date'], errors='coerce')
            customer_info['effective_end_date'] = datetime.strptime('3000-12-31', "%Y-%m-%d") 
            customer_info['is_current'] =  True
            customer_info = customer_info.drop_duplicates()

            # 3 --> Not null Checks ---
            transactions_check = number_of_null_values(transactions, 'transaction_id')
            customer_info_check = number_of_null_values(customer_info, 'customer_id')

            if transactions_check == False or customer_info_check == False:
                print(f'Key values for transaction_id and Customer_id in file {gcs_path} not present in all rows. Please review')
            else:
                #  4 --> Write all daily to transactions table
                succesfull_transactions_write = big_query_write(project_id=PROJECT_ID, dataset=DATA_SET, table=TRANSACTIONS_TABLE, BQ_client=bq_client, df=transactions, date=date)
                if succesfull_transactions_write:
                    print(f'Transactions for {date} writen succesfully')
                else:                    
                     print(f'Transactions for {date} writen failed')                                               
                # 5 --> Write to stagin table table
                sucessfull_staging_write = big_query_write(project_id=PROJECT_ID, dataset=DATA_SET, table=CUSTOMER_INFO_STAGING_TABLE, BQ_client=bq_client, df=customer_info, date=date)

                if sucessfull_staging_write:
                    # 6 --> SCD2 with target table
                    merge_scd2_bq(project_id=PROJECT_ID, dataset=DATA_SET, staging_table = CUSTOMER_INFO_STAGING_TABLE, target_table =CUSTOMER_INFO_TARGET_TABLE , BQ_client = bq_client)
                     # 7 --> Move processed file to Archive Folder in GCS
                    move_json_between_buckets(GCS_client = GCS_client , src_bucket_name = BUCKET_NAME,  dst_bucket_name = ARCHIVE_BUCKET, date = date)
                else:
                    print('Error in Staggin table, NOt performning SCD2')
               
    else:
        print('No new files to process ... ')

# if __name__ == '__main__':
#     main()