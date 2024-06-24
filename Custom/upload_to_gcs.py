from google.cloud import storage
from google.oauth2 import service_account
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
import os
import json

@custom
def upload_to_gcs(df, *args, **kwargs):
    # Load the config file
    config_path = os.path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    config = ConfigFileLoader(config_path, config_profile).config

    # Access the service account key from the config
    gcp_credentials = config['GOOGLE_SERVICE_ACC_KEY']
    
    # Parse the service account key JSON
    credentials = service_account.Credentials.from_service_account_info(gcp_credentials)

    # Initialize the storage client with the fetched credentials
    storage_client = storage.Client(credentials=credentials)
    
    bucket_name = 'daily-weather-partitions'
    bucket = storage_client.bucket(bucket_name)
    date = df['date'].iloc[0].strftime('%Y-%m-%d')
    year, month, day = date.split('-')
    partition_blob = f'data/year={year}/month={month}/day/{day}/weather_data_{date}.parquet'
    
    temp_dir = 'tmp'
    os.makedirs(temp_dir, exist_ok=True)
    temp_file = os.path.join(temp_dir, f'weather_data_{date}.parquet')
    df.to_parquet(temp_file, index=False)
    
    partition = bucket.blob(partition_blob)
    if partition.exists():
        print(f"File {partition_blob} already exists. Overwriting now.")
    partition.upload_from_filename(temp_file)
    print(f'File uploaded to {partition_blob}.')
    
    if os.path.exists(temp_file):
        os.remove(temp_file)
        print(f'Temporary file {temp_file} deleted.')

    return bucket_name