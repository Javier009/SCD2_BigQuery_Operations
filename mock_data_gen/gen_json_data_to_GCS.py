import argparse
import json
import random
from datetime import datetime, timedelta
from faker import Faker

from retrive_data_from_gcs import *
# from customers_info_fetch_sample import *
from change_customers_info import *

from google.cloud import storage

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


# Here main starts
def main():

    # p = argparse.ArgumentParser()
    # p.add_argument("--date", help="yyyy-mm-dd to process", required=False)
    # args = p.parse_args()
    # # dates_range = [args.date] or datetime.utcnow().strftime("%Y-%m-%d")
    # dates_range = [args.date] or generate_date_list('2025-01-01', '2025-05-31')
    
    BUCKET_NAME = 'transactions_raw_data'
    ARCHIVE_BUCKET = 'transactions_raw_data_archive'
    client = storage.Client()
    
    bucket_obj = client.get_bucket(BUCKET_NAME)
    archive_bucket_obj = client.get_bucket(ARCHIVE_BUCKET)
    
    dates_range = generate_date_list('2025-01-21', '2025-01-30')

    for date in dates_range:

        NUM_CUSTOMERS = random.randint(50,200)
        
        # Fetch a random sample of custumers that info will change
        files = list_gcs_files(ARCHIVE_BUCKET, client=client)
        customers_to_change_info = random_customers_that_info_will_change(files, bucket_obj=archive_bucket_obj)

        # customers_to_change_info = fetch_random_customers_for_info_change() 
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

if __name__ =='__main__':
    main()