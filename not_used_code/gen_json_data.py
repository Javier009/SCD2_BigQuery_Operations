import json
import random
from datetime import datetime, timedelta
from faker import Faker

from google.cloud import storage
from google.cloud import bigquery

PROJECT_ID  = "bigquery-dataflow-460522"

# Google Cloud Storage Bucket
STORAGE_BUCKET = 'retail_data_v1'
storage_client = storage.Client()
bucket_instance = storage_client.bucket(STORAGE_BUCKET)

fake = Faker('en_US') # Use US locale for addresses

# --- Configuration ---
NUM_CUSTOMERS = 200 # Total unique customers
TRANSACTIONS_PER_DAY_PER_CUSTOMER = 1 # Average transactions per day per customer
DATE_RANGE_DAYS = 7 # Generate data for 7 days to test SCD2

# GCS path where daily JSONs will be uploaded
GCS_DAILY_TRANSACTIONS_PREFIX = "gs://fin_txn_data/daily/transactions_"

# Attributes that will trigger SCD2 changes if they change
SCD2_TRACKED_ATTRIBUTES = ['customer_address', 'customer_city', 'customer_state', 'customer_zip', 'customer_tier']

# --- Initial Customer Profiles (for consistent generation) ---
# We'll store a "master" list of customer profiles and mutate them for daily files
# This ensures a consistent set of customers across runs if you re-run with same start_date
initial_customer_profiles = {}
for i in range(NUM_CUSTOMERS):
    cust_id = f"CUST{i:04d}"
    initial_customer_profiles[cust_id] = {
        "customer_id": cust_id,
        "customer_name": fake.name(),
        "customer_address": fake.street_address(),
        "customer_city": fake.city(),
        "customer_state": fake.state_abbr(),
        "customer_zip": fake.postcode(),
        "customer_tier": random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
        "customer_email": fake.email()
    }

# --- Function to generate a single transaction ---
def generate_transaction(customer_profile, transaction_date_str):
    transaction_time = fake.time_object().strftime('%H:%M:%S')
    transaction_timestamp = f"{transaction_date_str}T{transaction_time}Z"
    amount = round(random.uniform(5.00, 1000.00), 2)

    # Introduce data quality issues occasionally
    if random.random() < 0.02: # 2% chance of null amount
        amount = None
    elif random.random() < 0.01: # 1% chance of zero amount
        amount = 0.0
    elif random.random() < 0.005: # 0.5% chance of negative amount
        amount = -round(random.uniform(1.00, 50.00), 2)
    elif random.random() < 0.002: # 0.2% chance of amount as string
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
        "customer_info": customer_profile # Embed current customer profile
    }

# --- Main Data Generation Loop ---
def generate_daily_transaction_data(start_date, num_days):
    """
    Generates daily transaction JSON files for a given date range.
    Simulates SCD2 changes for a subset of customers each day.
    """
    current_customer_states = initial_customer_profiles.copy() # Start with initial profiles

    for day_offset in range(num_days):
        current_date = start_date + timedelta(days=day_offset)
        current_date_str = current_date.strftime('%Y-%m-%d')
        daily_transactions = []

        # Simulate some customers changing attributes for SCD2
        customers_to_change = random.sample(list(current_customer_states.keys()), k=random.randint(5, 15)) # 5-15 customers change per day
        for cust_id in customers_to_change:
            # Randomly change one of the SCD2 tracked attributes
            attribute_to_change = random.choice(SCD2_TRACKED_ATTRIBUTES)
            
            if attribute_to_change == 'customer_address':
                current_customer_states[cust_id][attribute_to_change] = fake.street_address()
                current_customer_states[cust_id]['customer_city'] = fake.city() # Often city/state/zip change with address
                current_customer_states[cust_id]['customer_state'] = fake.state_abbr()
                current_customer_states[cust_id]['customer_zip'] = fake.postcode()
            elif attribute_to_change == 'customer_tier':
                current_customer_states[cust_id][attribute_to_change] = random.choice([t for t in ['Bronze', 'Silver', 'Gold', 'Platinum'] if t != current_customer_states[cust_id][attribute_to_change]])
            # Add more logic here for other attributes if needed

        # Generate transactions for the day
        for cust_id, profile in current_customer_states.items():
            # Generate a few transactions per customer to make the file size reasonable
            num_transactions = random.randint(1, TRANSACTIONS_PER_DAY_PER_CUSTOMER)
            for _ in range(num_transactions):
                daily_transactions.append(generate_transaction(profile, current_date_str))

        # Shuffle daily transactions to mix up customer data
        random.shuffle(daily_transactions)

        # Output the JSON file
        output_filename = f"transactions_{current_date_str}.json"
        output_filepath = f"daily_data/{output_filename}" # Save to a local dir 'daily_data'
        
        # Ensure the directory exists
        import os
        os.makedirs("daily_data", exist_ok=True)

        with open(output_filepath, 'w') as f:
            json.dump(daily_transactions, f, indent=2)

        print(f"Generated {len(daily_transactions)} transactions for {current_date_str} in {output_filepath}")

# --- Run Data Generation ---
if __name__ == "__main__":
    start_date_for_generation = datetime(2025, 1, 1) # Start generating from Jan 1, 2025
    print(f"Generating {DATE_RANGE_DAYS} days of data starting from {start_date_for_generation.strftime('%Y-%m-%d')}...")
    generate_daily_transaction_data(start_date_for_generation, DATE_RANGE_DAYS)
    print("\nData generation complete. You can now upload these files to GCS.")
    print(f"Example GCS path for upload: {GCS_DAILY_TRANSACTIONS_PREFIX}YYYY_MM_DD.json")