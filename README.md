# SCD2 in BigQuery Pipeline

Pipeline that generates mock data into GCS and processes it with PySpark using Dataproc to perform SCD2 operations.

## Financial Transactions Data Processing Project

This project establishes a **serverless data engineering pipeline** on Google Cloud Platform (GCP). It processes daily financial transaction data, including changes in customer metadata, from Google Cloud Storage (GCS) into BigQuery, maintaining a **Slowly Changing Dimension Type 2 (SCD2)** for customer profiles.  
The entire pipeline is orchestrated via Pub/Sub messaging, ensuring an event-driven and decoupled workflow.

---

## 1. Project Overview

This pipeline ingests raw JSON transaction files from GCS.  
It's designed to specifically track changes in customer metadata embedded within these transactions, applying an SCD Type 2 strategy in BigQuery for historical record-keeping.  
The process is initiated by Pub/Sub messages, promoting a robust and asynchronous data flow.

---

## 2. Architecture

**High-Level Architecture:**

```
+---------------------+      +-----------------+      +------------------+      +------------------+
| Data Generation     |----->| Raw Data        |----->| Cloud Function   |----->| BigQuery         |
| (Writes to GCS)     |      | (GCS Bucket)    |      | (Python app)     |      | (Data Warehouse) |
| (Publishes to Pub/Sub)|    | `transactions_` |      | (Triggered by    |      | - daily_transactions
|                     |      | `raw_data`      |      | Pub/Sub)         |      | - customer_profile_scd2
+---------------------+      +-----------------+      +------------------+      +------------------+
                                     |                     ^
                                     |                     |
                                     |      +------------------+
                                     +----->| Archive Bucket   |-----+
                                            | `transactions_`  |
                                            | `raw_data_archive`
                                            +------------------+
```

---

## 3. Core Components

- **Google Cloud Storage (GCS):** For raw data and archives
- **Google Cloud Pub/Sub:** Triggers the pipeline
- **BigQuery:** Data warehouse with SCD2 history
- **Cloud Functions:** Serverless compute
- **Python Libraries:** pandas, google-cloud-storage, google-cloud-bigquery, gcsfs, pyarrow, functions-framework

---

## 4. Setup and Deployment

### Prerequisites

- GCP project
- Authenticated gcloud CLI
- Python 3.9+
- pip

### GCP Project Setup

- Enable required APIs (Cloud Storage, BigQuery, Cloud Functions, Pub/Sub)
- Create GCS buckets for raw and archived data
- Create a Pub/Sub topic
- Create BigQuery dataset and tables
- Grant required IAM roles to Cloud Function's service account

### Local Setup

```bash
git clone <your-repo-url>
cd <repo-directory>
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
gcloud auth application-default login
```

### Deployment to Cloud Functions

- Configure environment variables in `main.py`
- Deploy:

```bash
gcloud functions deploy <function-name> \
  --runtime python39 \
  --entry-point main \
  --trigger-topic <pubsub-topic> \
  --memory 2048MB \
  --timeout 540s
```

---

## 5. Project Structure

```
project-root/
│
├── main.py
├── requirements.txt
├── utils/
│   ├── read_data.py
│   ├── checks.py
│   ├── writer.py
├── README.md
└── ...
```

---

## 6. How it Works

- Pub/Sub triggers the Cloud Function.
- The function reads new files from GCS, transforms and cleans data, deduplicates customer records, and appends to BigQuery.
- Customer profiles are updated using an SCD2 merge.
- Processed files are archived in GCS.

---

## 7. Key Features

- Serverless and scalable
- Event-driven via Pub/Sub
- Automated ingestion from GCS
- Data transformation and SCD2 history
- Error handling and logging

---

## 8. Troubleshooting

- Initialize GCP client objects in `main()`, not globally.
- Add `pyarrow` to requirements if needed.
- Grant required IAM roles to the Cloud Function's service account.
- Ensure `read_data` returns a DataFrame (check for None).
- Increase memory or timeout in Cloud Function if needed.

---

## 9. Future Enhancements

- More robust error handling (dead-letter queues)
- Stricter schema validation
- Advanced monitoring and alerting
- CI/CD automation
- Comprehensive tests

---

## Notes

- **NUMERIC Columns:**  
  The `amount` column may be stored as `STRING` in BigQuery.  
  Analysts: cast to NUMERIC in SQL as needed:
  ```sql
  SELECT CAST(amount AS NUMERIC) FROM financial_transactions.daily_transactions
  ```

---

*Happy data engineering!* 🚀
