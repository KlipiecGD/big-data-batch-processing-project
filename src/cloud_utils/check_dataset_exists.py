from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.logging_utils.logger import logger

def ensure_dataset_exists(dataset_id: str, location: str = "US"):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)

    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_id} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        logger.info(f"Created dataset {dataset_id} in {location}.")

