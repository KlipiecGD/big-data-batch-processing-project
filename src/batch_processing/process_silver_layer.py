import os
import shutil
from pyspark.sql import SparkSession
from src.config.config import config
from src.logging_utils.logger import logger
from src.batch_processing.clean_data import clean_data
from dotenv import load_dotenv

load_dotenv()


def run_silver_layer_transformations(
    load_from_cloud: bool = config.data_generation.get("load_from_cloud", True),
    save_to_cloud: bool = config.data_generation.get("save_to_cloud", True),
    save_locally: bool = config.data_generation.get("save_locally", False),
) -> None:
    """
    Ingest cleaned data from bronze layer CSV files stored locally or in cloud storage and save to silver layer as Parquet files.

    Architecture:
    - Read CSVs from bronze layer data directory or from cloud storage
    - Clean data using Spark
    - Save cleaned data as Parquet files in silver layer directory locally or to cloud storage
    Args:
        load_from_cloud (bool): Whether to load the bronze layer files from cloud storage. If False, load from local storage.
        save_to_cloud (bool): Whether to save the silver layer files to cloud storage
        save_locally (bool): Whether to save the silver layer files locally
    """
    spark = (
        SparkSession.builder.appName("SilverLayerTransformations")
        .config(
            "spark.jars.packages",
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5",
        )
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            os.getenv(
                "GOOGLE_APPLICATION_CREDENTIALS"
            ),  # We need to pass path to GCP credentials JSON
        )
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    ingestion_order = config.required_tables.get("creation_order", [])
    bucket_name = config.cloud.get("gcs_bucket_name", "big-data-bucket-123456")

    # Define base paths
    local_bronze = config.data_generation.get("bronze_layer_path", "bronze_layer/")
    local_silver = config.data_generation.get("silver_layer_path", "silver_layer/")
    cloud_bronze = f"gs://{bucket_name}/bronze_layer"
    cloud_silver = f"gs://{bucket_name}/silver_layer"

    logger.info("Starting silver layer transformations...")

    cleaned_dfs = {}

    try:
        for table in ingestion_order:
            logger.info(f"Processing table: {table}")

            # Dynamic Input Path: Cloud vs Local
            if load_from_cloud:
                input_path = f"{cloud_bronze}/{table}.csv"
            else:
                input_path = os.path.join(local_bronze, f"{table}.csv")

            # Read CSV using Spark
            df = spark.read.csv(input_path, header=True, inferSchema=True)

            # Log record counts before and after cleaning - action triggers computation - only for debugging
            # initial_count = df.count()
            # logger.info(f"Initial record count for {table}: {initial_count}")

            # Clean the data
            df = clean_data(df, table)

            # Referential Integrity: Filter transactions based on cleaned users and products
            if table == "transactions":
                if "users" in cleaned_dfs and "products" in cleaned_dfs:
                    # Get valid user_ids and product_ids
                    valid_user_ids = cleaned_dfs["users"].select("user_id").distinct()
                    valid_product_ids = (
                        cleaned_dfs["products"].select("product_id").distinct()
                    )

                    # Filter transactions to keep only those with valid foreign keys
                    # Small tables are automatically broadcasted by Spark
                    df = df.join(valid_user_ids, on="user_id", how="inner")
                    df = df.join(valid_product_ids, on="product_id", how="inner")

                    logger.info(
                        f"Applied referential integrity filters for transactions"
                    )

            # Count is an action that triggers computation - only for debugging
            # cleaned_count = df.count()
            # removed_count = initial_count - cleaned_count
            # logger.info(f"Cleaned record count for {table}: {cleaned_count} (removed {removed_count} records)")

            # Cache DataFrames that will be reused in referential integrity checks
            if table in ["users", "products"]:
                df.cache()
                logger.info(f"Cached dataframe for table: {table}")

            # Store cleaned dataframe for referential integrity checks
            cleaned_dfs[table] = df

            # Dynamic Output: Save Locally
            if save_locally:
                local_output = os.path.join(local_silver, table)
                # Remove existing directory if it exists
                if os.path.exists(local_output):
                    shutil.rmtree(local_output)
                # Save as Parquet with Snappy compression
                df.write.option("compression", "snappy").parquet(
                    local_output, mode="overwrite"
                )
                logger.info(f"Saved locally to {local_output}")

            # Dynamic Output: Save to Cloud Directly
            if save_to_cloud:
                cloud_output = f"{cloud_silver}/{table}"
                # Save as Parquet with Snappy compression
                df.write.option("compression", "snappy").parquet(
                    cloud_output, mode="overwrite"
                )
                logger.info(f"Saved to cloud at {cloud_output}")

        logger.info("Silver layer processing completed successfully")

    except Exception as e:
        logger.error(f"Silver layer parquet generation failed: {e}")
        raise e
    finally:
        # Safely unpersist if the table exists and was cached
        for table in ["users", "products"]:
            df = cleaned_dfs.get(table)
            if df is not None:
                try:
                    df.unpersist()
                    logger.info(f"Unpersisted dataframe for table: {table}")
                except Exception as e:
                    logger.warning(f"Could not unpersist {table}: {e}")

        if "spark" in locals() and spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    run_silver_layer_transformations()
