from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.logging_utils.logger import logger
from src.config.config import config


def ensure_dataset_exists(dataset_id: str, location: str = config.cloud.get("default_location", "US")) -> None:
    """
    Ensure that a BigQuery dataset exists; create it if it does not.
    Args:
        dataset_id (str): The ID of the dataset to check/create.
        location (str): The location for the dataset if it needs to be created.
    """
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
