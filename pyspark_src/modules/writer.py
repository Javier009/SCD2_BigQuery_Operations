from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, current_date, lit


def pyspark_to_bigquery(project, dataset, table_id, df, mode):
    """
    Overwrite the staging table in BigQuery.
    """
    df.write \
      .format("bigquery") \
      .option("table", f"{project}.{dataset}.{table_id}") \
      .option("temporaryGcsBucket", "spark-staging-bucket-transactions") \
      .mode(mode) \
      .save()

def merge_scd2_bq(
    project_id: str,
    dataset: str,
    staging_table: str,
    target_table: str,
    BQ_client
):
    """
    Expire old SCD2 rows and upsert new/changed ones directly in BigQuery.
    """
    key  = "customer_id"
    cols = ["customer_address", "customer_city", "customer_state", "customer_zip", "customer_tier", "customer_email"]
    on   = f"T.{key} = S.{key} AND T.is_current"
    changes = " OR ".join(f"T.{c} <> S.{c}" for c in cols)

    merge_sql = f"""
    MERGE `{project_id}.{dataset}.{target_table}` AS T
    USING `{project_id}.{dataset}.{staging_table}` AS S
      ON {on}
    WHEN MATCHED AND ({changes}) THEN
      UPDATE SET
        T.is_current = FALSE,
        T.effective_end_date = S.effective_start_date 
    """

    # kick off the job on BigQuery
    job  = BQ_client.query(merge_sql)
    job.result()
    insert_query = f"""INSERT INTO `{project_id}.{dataset}.{target_table}` SELECT * FROM `{project_id}.{dataset}.{staging_table}`"""
    insert_staging_job = BQ_client.query(insert_query)
    insert_staging_job.result()  # wait for it to finish
    print(f"Merge completed: {job.job_id}")
    print(f"Rows inserted from staging table {staging_table} table to target table {target_table}")

def move_json_between_buckets(GCS_client, src_bucket_name: str, dst_bucket_name: str, date: str):
    blob_name = f"transactions_{date}.json"
    src_bucket = GCS_client.bucket(src_bucket_name)
    src_blob = src_bucket.blob(blob_name)
    dst_bucket = GCS_client.bucket(dst_bucket_name)
    dst_bucket.copy_blob(src_blob, dst_bucket, blob_name)
    src_blob.delete()

#   FUNCTIONS FOR LOCAL TESTS
def big_query_write_test(project_id, dataset, table, BQ_client, df, overwrite:bool):

    table_id = f'{project_id}.{dataset}.{table}'
    df_dict = [row.asDict() for row in df.collect()]

    #Write to BIgQuery Table
    if overwrite:        
        query = f"TRUNCATE TABLE `{table_id}`"
        BQ_client.query(query).result()

    errors = BQ_client.insert_rows_json(table_id, df_dict)
    if errors == []:
        print(f"Loaded {len(df_dict)} to {table_id}.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))