import re
import pandas as pd
import gcsfs


def list_gcs_files_dates(bucket_name, GCS_client, prefix=None):

    file_names_dates = []
    blobs = GCS_client.list_blobs(bucket_name, prefix=prefix)

    # print(f"Listing files in bucket: '{bucket_name}' (prefix: '{prefix if prefix else 'None'}')")
    for blob in blobs:
        match = re.search(r"transactions_(\d{4}-\d{2}-\d{2})\.json", blob.name)
        if match:    
            date_str = match.group(1)
            file_names_dates.append(date_str)
        else:
            pass
    return file_names_dates


def read_data(storage_file_system_object, file_path):
    try:
        with storage_file_system_object.open(file_path) as f:
            df = pd.read_json(f, lines=True) 
            return df
    except Exception as e:
       return  f"❌ Error REading Data: {e}"