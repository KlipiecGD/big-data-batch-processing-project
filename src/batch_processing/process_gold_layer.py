import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.config.config import config
from src.logging_utils.logger import logger
from src.cloud_utils.check_dataset_exists import ensure_dataset_exists

load_dotenv()


def run_gold_layer_creation(
    load_from_cloud: bool = config.data_generation.get("load_from_cloud", True),
) -> None:
    """
    Transform silver layer parquet files into gold layer analytical tables in BigQuery.

    Architecture:
    - Read cleaned data from silver layer Parquet files stored locally or in cloud storage
    - Perform Spark SQL transformations (aggregations, joins, window functions)
    - Write results to BigQuery tables in the gold layer dataset

    Args:
        load_from_cloud (bool): Whether to load the silver layer files from cloud storage. If False, load from local storage.
    """
    spark = None

    try:
        logger.info("Starting gold layer transformation...")

        # Initialize Spark Session
        spark = (
            SparkSession.builder.appName("GoldLayerToBigQuery")
            .config(
                "spark.jars.packages",
                (
                    "com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.43.1,"
                    "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5"
                ),
            )
            .config(
                "spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            )
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
            )
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )

        # Dynamic path definition based on load source
        if load_from_cloud:
            silver_path = f"gs://{config.cloud.get('gcs_bucket_name', 'big-data-bucket-123456')}/silver_layer/"
            logger.info("Loading silver layer data from Cloud Storage bucket")
        else:
            silver_path = config.data_generation.get(
                "silver_layer_path", "silver_layer/"
            )
            logger.info("Loading silver layer data from local storage")

        # Load data from Parquet files
        logger.info("Loading and caching silver layer Parquet files...")

        # Load transactions (large fact table)
        transactions = spark.read.parquet(
            os.path.join(silver_path, "transactions"), inferSchema=True
        )

        # Cache for performance
        transactions.cache()
        transactions.createOrReplaceTempView("transactions")

        # Count is an action that triggers computation - only for debugging
        # logger.info(f"Transactions loaded: {transactions.count()} records")

        # Load users (dimension table)
        users = spark.read.parquet(
            os.path.join(silver_path, "users"), inferSchema=True    
        )

        # Cache for performance
        users.cache()
        users.createOrReplaceTempView("users")

        # Count is an action that triggers computation - only for debugging
        # logger.info(f"Users loaded: {users.count()} records")

        # Load products (dimension table)
        products = spark.read.parquet(
            os.path.join(silver_path, "products"), inferSchema=True
        )

        # Cache for performance
        products.cache()
        products.createOrReplaceTempView("products")

        # Count is an action that triggers computation - only for debugging
        # logger.info(f"Products loaded: {products.count()} records")

        logger.info("Data loaded from silver layer successfully")

        # Get BigQuery dataset name and ensure it exists
        bq_dataset = config.cloud.get("bq_gold_layer_dataset", "gold_layer")
        ensure_dataset_exists(bq_dataset)

        # Process each query file
        query_files = config.queries.get("query_files", [])
        logger.info(f"Processing {len(query_files)} gold layer transformations...")

        for query_file in query_files:
            try:
                logger.info(f"Processing query: {query_file}")

                # Read SQL query
                query_path = os.path.join("sql_queries", query_file)
                if not os.path.exists(query_path):
                    logger.error(f"Query file not found: {query_path}")
                    continue

                with open(query_path, "r") as f:
                    query_sql = f.read()

                report_name = query_file.split(".")[0]

                # Execute Spark SQL transformation
                logger.info(f"Executing Spark SQL transformation for {report_name}...")
                result_df = spark.sql(query_sql)

                # Result tables are smaller - use coalesce to reduce partitions
                result_df = result_df.coalesce(1)

                try:
                    logger.info(f"Saving {report_name} to BigQuery...")
                    result_df.write.format("bigquery").option(
                        "table", f"{bq_dataset}.{report_name}"
                    ).option(
                        "temporaryGcsBucket", config.cloud.get("gcs_bucket_name")
                    ).mode("overwrite").save()
                    logger.info(f"Successfully saved {report_name} to BigQuery")
                except Exception as e:
                    logger.error(f"Failed to save {report_name} to BigQuery: {e}")
                    raise e

            except Exception as e:
                logger.error(f"Failed to process {query_file}: {e}")
                raise e

        logger.info("Gold layer transformation completed successfully")

    except Exception as e:
        logger.error(f"Gold layer transformation failed: {e}")
        raise

    finally:
        # Safely unpersist if variables were actually assigned
        for df_name in ["transactions", "users", "products"]:
            df = locals().get(df_name)
            if df is not None:
                try:
                    df.unpersist()
                    logger.info(f"Unpersisted {df_name}")
                except Exception:
                    pass

        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    run_gold_layer_creation()
