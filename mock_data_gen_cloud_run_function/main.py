import argparse
import json
import random
from datetime import datetime, timedelta
from faker import Faker
from flask import Request

from utils.retrive_data_from_gcs import *
from utils.change_customers_info import *

from google.cloud import storage
from google.cloud import pubsub_v1

PROJECT_ID = 'dataproc-spark-461405'

fake = Faker()

def generate_new_customer_profiles():
      
      return {
        "customer_id": f'CUST_{fake.uuid4()}',
        "customer_name": fake.name(),
        "customer_address": fake.street_address(),
        "customer_city": fake.city(),
        "customer_state": fake.state_abbr(),
        "customer_zip": fake.postcode(),
        "customer_tier": random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
        "customer_email": fake.email()
    }

def generate_transaction(customer_profile, transaction_date_str):
    transaction_time = fake.time_object().strftime('%H:%M:%S')
    transaction_timestamp = f"{transaction_date_str}T{transaction_time}Z"
    amount = round(random.uniform(5.00, 1000.00), 2)

    # Introduce data quality issues occasionally
    if random.random() < 0.02: 
        amount = None
    elif random.random() < 0.01: 
        amount = 0.0
    elif random.random() < 0.005: 
        amount = -round(random.uniform(1.00, 50.00), 2)
    elif random.random() < 0.002: 
        amount = str(amount)

    return {
        "transaction_id": fake.uuid4(),
        "transaction_timestamp": transaction_timestamp,
        "amount": amount,
        "currency": "USD",
        "merchant_id": fake.bothify(text='MERCH###??'),
        "merchant_category": random.choice(['Groceries', 'Online Retail', 'Travel', 'Restaurants', 'Utilities', 'Electronics', 'Entertainment']),
        "card_type": random.choice(['Visa', 'Mastercard', 'Amex', 'Discover']),
        "transaction_status": random.choice(['approved', 'denied', 'pending']),
        "customer_info": customer_profile 
    }

def generate_date_list(start_date_str, end_date_str):
    date_list = []
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        print("Error: Ensure dates are in 'YYYY-MM-DD' format.")
        return date_list

    if start_date > end_date:
        print("Error: Start date cannot be after end date.")
        return date_list

    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    return date_list

def upload_json_to_gcs(gcs_bucket_obj, file_name, json_data):
    blob = gcs_bucket_obj.blob(file_name) 
    try:
        # json_string = json.dumps(json_data, indent=2)
        json_string = "\n".join(json.dumps(record) for record in json_data)
        blob.upload_from_string(json_string, content_type='application/json')

        print(f"Successfully uploaded '{file_name}' to GCS bucket '{gcs_bucket_obj.name}'.")

    except Exception as e:
        print(f"Error uploading '{file_name}' to GCS: {e}")


def send_message_to_pubsub(project_id, topic_id, publisher_obj, data):
    try:
        topic_path = publisher_obj.topic_path(project_id, topic_id)    
        message = {"Files to be processed": data}
        message_bytes = json.dumps(message).encode("utf-8")
        print(message)
        future = publisher_obj.publish(topic_path, message_bytes)
        print(f"Published message ID: {future.result()}")
    except Exception as e:
        print(f"Error publishing message: {e}")

# Here main starts
def main(request: Request):

    # p = argparse.ArgumentParser()
    # p.add_argument("--date", help="yyyy-mm-dd to process", required=False)
    # args = p.parse_args()
    # # dates_range = [args.date] or datetime.utcnow().strftime("%Y-%m-%d")
    # dates_range = [args.date] or generate_date_list('2025-01-01', '2025-05-31')
    try: 
        # GCS storage variables
        BUCKET_NAME = 'transactions_raw_data'
        ARCHIVE_BUCKET = 'transactions_raw_data_archive'
        client = storage.Client()

        # Pub/Sub variables
        TOPIC_ID = 'transactions-available-data'
        publisher = pubsub_v1.PublisherClient()
        
        bucket_obj = client.get_bucket(BUCKET_NAME)
        archive_bucket_obj = client.get_bucket(ARCHIVE_BUCKET)
        
        
        existing_files = list_gcs_files(ARCHIVE_BUCKET, client=client)
        latest_processed_date_str = get_latest_processed_date(existing_files)
        next_processed_date_dt_object = datetime.strptime(latest_processed_date_str, '%Y-%m-%d') + timedelta(days=1)
        next_date_to_process =  next_processed_date_dt_object.strftime('%Y-%m-%d')
        
        today_object = datetime.today()
        today_string = today_object.strftime('%Y-%m-%d')

        dates_range = generate_date_list(next_date_to_process, today_string)
    
        for date in dates_range:

            NUM_CUSTOMERS = random.randint(50,200)

            # Fetch a random sample of custumers that info will change        
            customers_to_change_info = random_customers_that_info_will_change(existing_files, bucket_obj=archive_bucket_obj)
            customer_profiles = customers_info_change(customers_to_change_info)

            # -- Fetch customers to change info + new customers --
            for n in range(NUM_CUSTOMERS):

                new_customer_profile = generate_new_customer_profiles()
                customer_profiles.append(new_customer_profile)

            # Generate transactions
            day_transactions = []

            for profile in customer_profiles:
                transactions = random.randint(1,2)
                for t in range(transactions):
                    transaction = generate_transaction(profile, date)
                    day_transactions.append(transaction)

            # Send to GCS
            upload_json_to_gcs(bucket_obj, f'transactions_{date}.json', day_transactions)

        # Fetch all existing files in GCS Cloud Storage
        # Publsh Message to Pub/Sub notifing about existing files intransactions bucket that need to be processed
        
        existing_transactions_files = list_gcs_files(BUCKET_NAME, client=client, full_path = True) 
        send_message_to_pubsub(PROJECT_ID, TOPIC_ID, publisher, existing_transactions_files)
       
        return  f'✅ Data ingestion was succesful  and message sent to PubSub Topic to trigger SCD2 operations', 200
    
    except Exception as e:
       return  f"❌ Error Generating Mock data: {e}", 500 

if __name__ =='__main__':
    main()