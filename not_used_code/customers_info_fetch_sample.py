import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, rand#, when, sum, count, lit

# --- Configuration for your BigQuery Table ---
PROJECT_ID = "dataproc-spark-461405"
DATASET_ID = "financial_transactions"
TABLE_ID = "customer_profile_scd2"

GCS_TEMP_BUCKET = " temp-bucket-spark-data-transfer"
BQ_TABLE_PATH = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def fetch_random_customers_for_info_change():
    # --- Initialize Spark Session ---
    # Ensure the BigQuery connector JAR is included.
    # The version '0.29.0' for scala 2.12 is generally compatible with Spark 3.x.
    spark = SparkSession.builder \
        .appName(f"Read_{TABLE_ID}") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "false") \
        .config("spark.cloud.google.temp.gcs.bucket", GCS_TEMP_BUCKET) \
        .getOrCreate()

    print("SparkSession created successfully.")

    # --- Read the BigQuery Table into a Spark DataFrame  ---
    try:
        df_customer_profile_scd2 = spark.read \
            .format("bigquery") \
            .option("table", BQ_TABLE_PATH) \
            .load() \
            .filter(col('is_current') == True) # Latest values so we only change the most recetn version
        print(f"\nSuccessfully loaded BigQuery table: {BQ_TABLE_PATH}")

        # --- Select X customers randomly for info change in the next da records  ---
        N_SAMPLE_PERCENT = random.randint(10,40)/100
        N_SAMPLE = round(N_SAMPLE_PERCENT * df_customer_profile_scd2.count())
        print(f"Changing {N_SAMPLE} customers info")
        df_sample_fraction = df_customer_profile_scd2.orderBy(rand())\
            .limit(N_SAMPLE)\
            .select("customer_id",
                    "customer_name",
                    "customer_address",
                    "customer_city",
                    "customer_state",
                    "customer_zip",
                    "customer_tier",
                    "customer_email")
        # df_sample_fraction.show()
        row_list = df_sample_fraction.collect()
        data_sample_list = [r.asDict() for r in row_list]
        
        return data_sample_list

    except Exception as e:
        print(f"An error occurred while reading BigQuery table: {e}")
        print("\nPossible issues:")
        print(f"- Ensure the BigQuery table '{BQ_TABLE_PATH}' exists and your user/service account has read permissions.")
        print(f"- Ensure the GCS temporary bucket '{GCS_TEMP_BUCKET}' exists and is writable/readable.")
        print("- Verify your Google Cloud authentication setup (e.g., `gcloud auth application-default login`).")
        print("- Check the version of the Spark BigQuery connector in `spark.jars.packages` for compatibility.")
        return []

    finally:
        # Stop the SparkSession when done
        spark.stop()
        print("\nSparkSession stopped.")