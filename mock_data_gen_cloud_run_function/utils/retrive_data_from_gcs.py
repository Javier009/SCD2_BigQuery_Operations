import argparse
import re
import json
import random
from datetime import datetime, timedelta
from faker import Faker

from Module_3_class_1.financial_transactions_project.mock_data_gen_cloud_run_function.utils.change_customers_info import *

from google.cloud import storage

# BUCKET_NAME  = 'transactions_raw_data'
# client = storage.Client()
# bucket = client.get_bucket(BUCKET_NAME)


def list_gcs_files(bucket_name, client, full_path = False, prefix=None):
    # client = storage.Client()
    # bucket = client.get_bucket(bucket_name)
    file_names = []
    blobs = client.list_blobs(bucket_name, prefix=prefix)

    print(f"Listing files in bucket: '{bucket_name}' (prefix: '{prefix if prefix else 'None'}')")
    if full_path:
        for blob in blobs:
            file_names.append(f'gs://{bucket_name}/{blob.name}')
    else:
        for blob in blobs:
            file_names.append(blob.name)
          

    return file_names


def get_latest_processed_date(files):
    dates = []
    for file in files:
        match = re.search(r"transactions_(\d{4}-\d{2}-\d{2})\.json", file)
        if match:    
            date_str = match.group(1)
            dates.append(date_str)
        else:
            pass
    latest_date = max(dates)
    return latest_date


def random_customers_that_info_will_change(list_of_files, bucket_obj):

    if len(list_of_files) > 0:
        all_files_customer_info_list = []

        if len(list_of_files) == 1:
            number_of_files = 1
        elif len(list_of_files) == 2:
            number_of_files = random.choice([1,2])
        elif len(list_of_files) == 3:
            number_of_files = random.choice([1,2,3])
        else:
            number_of_files = random.choice([1,2,3,4])
        print(number_of_files)

        random_files = random.sample(list_of_files, number_of_files)

        for file in random_files:
            blob = bucket_obj.blob(file)
            json_data_str = blob.download_as_text()
            # data = json.loads(json_data_str)
            data = data = [json.loads(line) for line in json_data_str.splitlines() if line.strip()]
            customer_info_data = [d['customer_info'] for d in data]
            for d in customer_info_data:
                all_files_customer_info_list.append(d)

        # print(len(all_files_customer_info_list))
        # Generate Unique random custumers
        final_selected_customers = []
        captured_customer_ids = []
        random_customers_to_info_change = round((random.randint(10, 20) / 100) * len(all_files_customer_info_list))
        print(f'Customers to info change: {random_customers_to_info_change}')
        
        iterator = 0
        while iterator < random_customers_to_info_change:
            selected_customer = all_files_customer_info_list[random.randint(0,len(all_files_customer_info_list)-1)]
            cutomer_id = selected_customer['customer_id']
            if cutomer_id not in captured_customer_ids:
                final_selected_customers.append(selected_customer)
                captured_customer_ids.append(cutomer_id)
                iterator += 1
            else:
                pass   

        return final_selected_customers
    else:
        return []

# files = list_gcs_files(BUCKET_NAME)
# data = random_customers_that_info_will_change(files)
# print(data)