import os
import shutil
from pyspark.sql import SparkSession
from src.config.config import config
from src.logging_utils.logger import logger
from src.batch_processing.clean_data import clean_data
from dotenv import load_dotenv

load_dotenv()


def run_silver_layer_transformations() -> None:
    """
    Ingest cleaned data from bronze layer CSV files and save to silver layer as Parquet files.
    
    Architecture:
    - Read CSVs from bronze layer data directory
    - Clean data using Spark
    - Save cleaned data as Parquet files in silver layer directory
    """
    spark = SparkSession.builder.appName("SilverLayerTransformations") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    # Get table order for processing
    ingestion_order = config.required_tables.get("creation_order", [])
    
    # Define paths
    bronze_path = config.data_generation.get("bronze_layer_path", "bronze_layer/")
    silver_path = config.data_generation.get("silver_layer_path", "silver_layer/")
    
    # Create silver_layer directory if it doesn't exist
    if not os.path.exists(silver_path):
        os.makedirs(silver_path)

    logger.info("Starting silver layer parquet files generation...")

    # Store cleaned dataframes for referential integrity checks
    cleaned_dfs = {}
    
    try:
        for table in ingestion_order:
            logger.info(f"Processing table: {table}")
            
            # Read CSV from bronze layer
            csv_path = os.path.join(bronze_path, f"{table}.csv")
            
            if not os.path.exists(csv_path):
                logger.error(f"CSV file not found: {csv_path}")
                raise FileNotFoundError(f"Missing bronze layer file: {csv_path}")
            
            df = spark.read.csv(csv_path, header=True, inferSchema=True)
            
            # Log record counts before and after cleaning
            initial_count = df.count()
            logger.info(f"Initial record count for {table}: {initial_count}")
            
            # Clean the data
            df = clean_data(df, table)
            
            # REFERENTIAL INTEGRITY: Filter transactions based on cleaned users and products
            if table == "transactions":
                if "users" in cleaned_dfs and "products" in cleaned_dfs:
                    # Get valid user_ids and product_ids
                    valid_user_ids = cleaned_dfs["users"].select("user_id").distinct()
                    valid_product_ids = cleaned_dfs["products"].select("product_id").distinct()
                    
                    # Filter transactions to keep only those with valid foreign keys
                    # Small tables are automatically broadcasted by Spark
                    df = df.join(valid_user_ids, on="user_id", how="inner")
                    df = df.join(valid_product_ids, on="product_id", how="inner")
                    
                    logger.info(f"Applied referential integrity filters for transactions")
            
            cleaned_count = df.count() # Count is an action that triggers computation, but we need to know how many records were removed
            removed_count = initial_count - cleaned_count
            logger.info(f"Cleaned record count for {table}: {cleaned_count} (removed {removed_count} records)")

            # Repartition data for optimized storage
            if cleaned_count > 10000:
                num_partitions = max(4, cleaned_count // 5000)
                df = df.repartition(num_partitions)
            else:
                df = df.coalesce(1)  # Small tables can be stored in a single file
            logger.info(f"Repartitioned {table} to {df.rdd.getNumPartitions()} partitions")

            # Cache DataFrames that will be reused in referential integrity checks
            if table in ["users", "products"]:
                df.cache()
                logger.info(f"Cached dataframe for table: {table}")

            # Store cleaned dataframe for referential integrity checks
            cleaned_dfs[table] = df
            
            # Save to Parquet in silver_layer/
            parquet_path = os.path.join(silver_path, table)
            
            # Remove existing parquet directory if it exists
            if os.path.exists(parquet_path):
                shutil.rmtree(parquet_path)
            
            # Write cleaned DataFrame to Parquet - optimize by compression
            df.write.option("compression", "snappy").parquet(parquet_path, mode="overwrite")
            
            logger.info(f"Successfully saved {cleaned_count} records to {parquet_path}")
        
        logger.info("Silver layer parquet generation completed successfully")
        
    except Exception as e:
        logger.error(f"Silver layer parquet generation failed: {e}")
        raise e
    finally:
        for table, df in cleaned_dfs.items():
            if table in ["users", "products"]:
                try:
                    df.unpersist()
                    logger.info(f"Unpersisted dataframe for table: {table}")
                except:
                    pass
        spark.stop()


if __name__ == "__main__":
    run_silver_layer_transformations()