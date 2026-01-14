import os
from google.cloud import storage
from dotenv import load_dotenv

from src.logging_utils.logger import logger

load_dotenv()

def save_files_to_bucket(file_paths: list[str], bucket_name: str, destination_folder: str) -> None:
    """
    Save local files to a Google Cloud Storage bucket.

    Args:
        file_paths (list[str]): List of local file paths to upload. Can include directories for Spark Parquet files.
        bucket_name (str): Name of the GCS bucket.
        destination_folder (str): Destination folder in the bucket.
    """
    # Instantiates a client
    storage_client = storage.Client()

    # Gets the existing bucket
    bucket = storage_client.bucket(bucket_name)

    for path in file_paths:
        if os.path.isdir(path):
            # If it's a directory (Spark Parquet), upload all files inside
            table_name = os.path.basename(path)
            for root, dirs, files in os.walk(path):
                for file in files:
                    local_file = os.path.join(root, file)
                    # Construct GCS path: silver_layer/table_name/part-0000...
                    relative_path = os.path.relpath(local_file, os.path.dirname(path))
                    blob = bucket.blob(f"{destination_folder}/{relative_path}")
                    blob.upload_from_filename(local_file)
            logger.info(f"Uploaded Parquet folder {table_name} to {bucket_name}")
        else:
            # Handle single files
            file_name = os.path.basename(path)
            blob = bucket.blob(f"{destination_folder}/{file_name}")
            blob.upload_from_filename(path)
            logger.info(f"Uploaded {file_name} to {bucket_name}")