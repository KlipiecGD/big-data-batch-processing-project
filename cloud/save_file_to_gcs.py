# Imports the Google Cloud client library
from google.cloud import storage

# Instantiates a client
storage_client = storage.Client()

# Gets the existing bucket
bucket = storage_client.bucket("big-data-project-bucket-123456")

blob = bucket.blob("bronze_layer/sample_file.txt")
blob.upload_from_filename(
    "/Users/klipiec/Desktop/Internship/projects/BigDataProject/cloud/sample_file.txt"
)
