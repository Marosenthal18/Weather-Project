from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
import os
import json

@custom
def create_bq_dataset(**kwargs):

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

    # Defining the dataset reference
    dataset_ref = bq_client.dataset(dataset_id)

    try:
        bq_client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        bq_client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")