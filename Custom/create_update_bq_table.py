from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
import os
import json
import pandas as pd


@custom
def create_or_update_bq_table(df, *args, **kwargs):

    # Defining the table_id
    table_id = 'weather_data'

    # Defining the dataset id
    dataset_id = 'weather_project'

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

    # Defining the references
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    # Generate BigQuery schema from DataFrame
    schema = []
    for column_name, dtype in df.dtypes.items():
        if column_name == 'date':
            field_type = "DATE"
        elif pd.api.types.is_integer_dtype(dtype):
            field_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            field_type = "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            field_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            field_type = "TIMESTAMP"
        else:
            field_type = "STRING"
        schema.append(bigquery.SchemaField(column_name, field_type, mode="NULLABLE"))

    try:
        table = bq_client.get_table(table_ref)
        existing_schema = {field.name: field.field_type for field in table.schema}
        new_columns = [field for field in schema if field.name not in existing_schema]

        if new_columns:
            print(f"Adding new columns: {[field.name for field in new_columns]}")
            table.schema += new_columns
            bq_client.update_table(table, ["schema"])
            print(f"Table {table_id} updated with new columns.")
        else:
            print(f"Table {table_id} already exists with the required schema.")
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="date")
        bq_client.create_table(table)
        print(f"Table {table_id} created.")