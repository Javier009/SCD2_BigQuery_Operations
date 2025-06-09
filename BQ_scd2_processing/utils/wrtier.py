from google.cloud import bigquery

def big_query_write(project_id, dataset, table, BQ_client, df, date):

    table_id = f'{project_id}.{dataset}.{table}'

    try:
        
        if table == 'daily_transactions':
            print('Transactions table ....')       
            query = f"DELETE FROM `{table_id}` WHERE transaction_date = '{date}'"
            BQ_client.query(query).result()
        else :
            query = f"TRUNCATE TABLE `{table_id}`"
            BQ_client.query(query).result()

        job_config = bigquery.LoadJobConfig()    
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job = BQ_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f'Succesfully wrote data in {table_id}, for date = {date}')

    except Exception as e:
       print( f"❌ Error writing to BigQuery: {e}")
       

def column_bq_test(project_id, dataset, table, BQ_client, df):   
    table_id = f'{project_id}.{dataset}.{table}'
    for col in df.columns:
        print(f"Testing column: {col}")
        try:
            BQ_client.load_table_from_dataframe(df[[col]].head(10), table_id)
            print(f"✅ Success: {col}")
        except Exception as e:
            print(f"❌ Error with column '{col}': {e}")