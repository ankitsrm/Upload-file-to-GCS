import glob
import os
import json
from google.cloud import storage

# Initialize the Google Cloud Storage client
gcsclient = storage.Client()

def get_list_of_files(src_directory):
    """Get a list of files with names ending with 'part-00000' in a directory."""
    items = glob.glob(f'{src_directory}/**', recursive=True)
    return list(filter(lambda item: os.path.isfile(item) and item.endswith('part-00000'), items))

def get_parquet_schema(schema_file, service):
    """Get Parquet schema from a JSON schema file for a specific service."""
    schema = json.load(open(schema_file))    
    order = schema[service]   
    print(order)

    # Sort columns based on column position
    ds_schema = sorted(schema[service], key=lambda col: col['column_position'])
    print(f"sorted == {ds_schema}")

    column_names = [col['column_name'] for col in ds_schema]
    print(f"List == {column_names}")
    print("==========================================")
    return column_names

def upload_file_to_gcs(filename, blob_name_file) -> str:
    """Upload a file to Google Cloud Storage."""
    bucket = gcsclient.bucket('pythongcsde')
    blob = bucket.blob(blob_name_file)
    blob.upload_from_filename(filename=filename)
    print("gs://pythongcsde/" + blob_name_file)
    return "gs://pythongcsde/" + blob_name_file

def read_csv_from_gcs(blob_name, schema_name):
    """Read a CSV file from Google Cloud Storage and return a DataFrame."""
    import pandas as pd 
    df = pd.read_csv(blob_name, names=schema_name)
    print(df)
    return df

# Source directory containing files to process
src_directory = "D:\\DE\\data-master\\data-master\\retail_db"

# Get a list of files to process
list_of_files = get_list_of_files(src_directory=src_directory)

# Process each file in the list
for file_path in list_of_files:
    print(file_path)
    
    # Extract service name from the file path
    service_name = file_path.split("\\")[-2]
    print(service_name)
    
    # Get the Parquet schema for the service
    schema_name = get_parquet_schema('D:\\DE\\data-master\\data-master\\retail_db\\schemas.json', service_name)
    
    # Generate blob name for Google Cloud Storage
    blob_name_file = '/'.join(file_path.split("\\")[4:])
    
    # Upload file to Google Cloud Storage
    blob_name = upload_file_to_gcs(filename=file_path, blob_name_file=blob_name_file)
    
    # Read CSV file from Google Cloud Storage
    read_csv_from_gcs(blob_name=blob_name, schema_name=schema_name)
