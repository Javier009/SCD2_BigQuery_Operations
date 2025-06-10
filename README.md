# dataproc-pyspark-bigquery-transactions-pipeline
Pipeline that genrates mock data into GCS and processes it with pyspark using dataproc to perform SCD2 opearations
Financial Transactions Data Processing Project
This project establishes a serverless data engineering pipeline on Google Cloud Platform (GCP). It processes daily financial transaction data, including changes in customer metadata, from Google Cloud Storage (GCS) into BigQuery, maintaining a Slowly Changing Dimension Type 2 (SCD Type 2) for customer profiles. The entire pipeline is orchestrated via Pub/Sub messaging, ensuring an event-driven and decoupled workflow.

1. Project Overview
This pipeline ingests raw JSON transaction files from GCS. It's designed to specifically track changes in customer metadata embedded within these transactions, applying an SCD Type 2 strategy in BigQuery for historical record-keeping. The process is initiated by Pub/Sub messages, promoting a robust and asynchronous data flow.

2. Architecture
The pipeline uses key GCP services for a scalable, serverless data architecture:

+---------------------+      +-----------------+      +------------------+      +------------------+
| Data Generation     |----->| Raw Data        |----->| Cloud Function   |      | BigQuery         |
| (Writes to GCS)     |      | (GCS Bucket)    |      | (Python app)     |----->| (Data Warehouse) |
| (Publishes to Pub/Sub)|      | `transactions_`   |      | (Triggered by    |      | - daily_transactions
|                     |      | `raw_data`      |      | Pub/Sub)         |      | - customer_profile_scd2
+---------------------+      +-----------------+      +------------------+      +------------------+
                                     |                     ^
                                     |                     |
                                     |      +------------------+
                                     +----->| Archive Bucket   |-----+ (Moves processed files)
                                            | `transactions_`  |
                                            | `raw_data_archive`
                                            +------------------+
Workflow:

Data Generation: An upstream process creates and uploads raw JSON transaction files (potentially with updated customer metadata) to a GCS bucket. It then publishes a Pub/Sub message.
Processing Trigger: A Cloud Function, subscribed to the Pub/Sub topic, is invoked by this message.
The Cloud Function reads the specified JSON data from GCS.
Data is transformed: timestamps are parsed, nested customer info is extracted, and customer changes are deduplicated to keep only the latest version within the batch.
Basic data quality checks are performed.
Transformed transaction data is appended to the daily_transactions table in BigQuery.
Customer profiles are processed via an SCD Type 2 MERGE operation in BigQuery, marking old records inactive and inserting new versions for changed customers.
Processed raw files are moved to an archive GCS bucket.
3. Core Components
Google Cloud Storage (GCS): For raw data (transactions_raw_data) and archives (transactions_raw_data_archive).
Google Cloud Pub/Sub: Decouples data generation from processing, triggering the Cloud Function.
BigQuery: The data warehouse (financial_transactions dataset) with tables for daily transactions and the SCD Type 2 customer profile.
Cloud Functions: The serverless compute environment for the Python processing application.
Python Libraries: pandas, google-cloud-storage, google-cloud-bigquery, gcsfs, pyarrow, functions-framework.
4. Setup and Deployment
Prerequisites
You need a GCP project, authenticated gcloud CLI, Python 3.9+, and pip.

GCP Project Setup
Enable the necessary GCP APIs (Cloud Storage, BigQuery, Cloud Functions, Pub/Sub).
Create two GCS buckets for raw data and archives.
Create a Pub/Sub topic to trigger the processing.
Create the BigQuery dataset (financial_transactions). Tables (daily_transactions, stg_daily_customer_info, customer_profile_scd2) can be created by the script, but defining the SCD2 schema beforehand is recommended.
Grant specific IAM roles to your Cloud Function's service account: Storage Object Viewer/Admin, BigQuery Data Editor/Job User, and Pub/Sub Subscriber.
Local Setup
Clone the project repository.
Set up a Python virtual environment.
Install all dependencies from requirements.txt.
Authenticate your local gcloud for GCP project access.
Deployment to Cloud Functions
Define your project configuration (bucket names, table names) as environment variables in main.py (reading from os.environ).
Deploy the Cloud Function, specifying Python 3.9 runtime, main as the entry point, and linking it to your Pub/Sub trigger topic. Set appropriate memory and timeout limits.
5. Project Structure
The project is organized with main.py as the entry point and a utils/ directory containing modular functions for reading data, performing checks, writing to BigQuery/GCS, and handling SCD Type 2 logic.

6. How it Works
Upon receiving a Pub/Sub message, the Cloud Function initializes GCP clients. It then identifies new files in GCS, reads them into Pandas DataFrames, and performs transformations like timestamp parsing and customer data extraction. It deduplicates customer information to ensure the latest state. After quality checks, transaction data is appended to BigQuery, and customer profiles are updated using an SCD Type 2 merge. Finally, processed raw files are moved to an archive bucket.

7. Key Features
Serverless: Scales automatically with demand, cost-efficient.
Event-Driven: Pub/Sub decouples components for robust asynchronous processing.
Automated Ingestion: Seamlessly reads JSON from GCS.
Data Transformation: Handles complex data preparation.
SCD Type 2: Maintains full historical records of customer profile changes.
Intraday Change Handling: Specifically manages multiple customer updates within a single batch.
Idempotent: Archived files prevent reprocessing.
Modular: Well-structured code for easy maintenance.
Error Handling: Includes basic error capture and logging.
8. Troubleshooting
This class is not fork-safe: Initialize GCP client objects within your main() function, not globally.
Requires pyarrow: Add pyarrow to your requirements.txt and redeploy.
403 Forbidden / Permission Denied: Grant necessary IAM roles (Storage, BigQuery, Pub/Sub) to your Cloud Function's service account.
TypeError: string indices must be integers: Ensure your read_data function returns a DataFrame (or None on error), and check for None before DataFrame operations.
Memory Exceeded: Increase the Cloud Function's allocated memory.
Timeout: Optimize processing logic or increase the Cloud Function's timeout limit (up to 9 minutes).
9. Future Enhancements
Consider adding more robust error handling (e.g., dead-letter queues), stricter schema validation, advanced data quality checks, comprehensive monitoring and alerting, and externalizing more configurations. Automating deployments with CI/CD and adding comprehensive tests would also enhance the project.