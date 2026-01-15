import pandas as pd

from google.cloud import storage
from src.logging_utils.logger import logger


def upload_dataframe_to_gcs(
    df: pd.DataFrame, bucket_name: str, destination_blob_name: str
) -> None:
    """
    Uploads a pandas DataFrame directly to GCS as a CSV string without local intermediate files.
    Args:
        df (pd.DataFrame): The DataFrame to upload.
        bucket_name (str): The name of the GCS bucket.
        destination_blob_name (str): The destination path in the bucket (including filename).
    """
    try:
        # 1. Initialize the client using Application Default Credentials or Service Account
        storage_client = storage.Client()

        # 2. Use the existing bucket
        bucket = storage_client.bucket(bucket_name)

        # 3. Create the blob object
        blob = bucket.blob(destination_blob_name)

        # 4. Convert DataFrame to CSV string
        csv_data = df.to_csv(index=False)

        # 5. Perform the direct string upload
        blob.upload_from_string(
            csv_data,
            content_type="text/csv",
        )

        logger.info(
            f"Successfully saved data to GCS: gs://{bucket_name}/{destination_blob_name}"
        )

    except Exception as e:
        logger.error(f"Failed to save data to GCS: {e}")
        raise
