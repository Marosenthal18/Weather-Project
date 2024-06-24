from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(bucket_name, **kwargs) -> None:
    
    # Defining the table_id
    table_id = 'weather_data'

    # Defining the dataset id
    dataset_id = 'weather_project'
    
    date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    year, month, day = date.split('-')

    partition_blob = f'data/year={year}/month={month}/day/{day}/weather_data_{date}.parquet'

    # Load the config file
    config_path = os.path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    config = ConfigFileLoader(config_path, config_profile).config

    # Access the service account key from the config
    gcp_credentials = config['GOOGLE_SERVICE_ACC_KEY']
    
    # Parse the service account key JSON
    credentials = service_account.Credentials.from_service_account_info(gcp_credentials)

    # Initialize the storage client with the fetched credentials
    bq_client =  bigquery.Client(credentials=credentials)

    table_ref = bq_client.dataset(dataset_id).table(table_id)

    # Check if data for the given date already exists in the table
    query = f"""
        SELECT COUNT(*) as count 
        FROM `{dataset_id}.{table_id}` 
        WHERE date = '{date}'
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    for row in results:
        if row['count'] > 0:
            print(f"Data for {date} already exists in {dataset_id}.{table_id}. Skipping load.")
            return
        
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(field="date")
    )

    uri = f'gs://{bucket_name}/{partition_blob}'
    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()

    print(f'Loaded data from {uri} into {dataset_id}.{table_id} partitioned by {date}')