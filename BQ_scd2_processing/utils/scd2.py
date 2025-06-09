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
        T.effective_end_date = CAST(S.effective_start_date AS DATE)
    """

    # kick off the job on BigQuery
    job  = BQ_client.query(merge_sql)
    job.result()

    insert_query = f"""
        INSERT INTO `{project_id}.{dataset}.{target_table}` 
        SELECT 
            customer_id,
            customer_name,
            customer_address,
            customer_city,
            customer_state,
            customer_zip,
            customer_tier,
            customer_email,
            CAST(effective_start_date AS DATE) AS effective_start_date,
            CAST(effective_end_date AS DATE) AS effective_end_date,
            is_current
        FROM `{project_id}.{dataset}.{staging_table}`
    """
    
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




