import os
import shutil
import time
from typing import Any
from pyspark.sql import SparkSession
from src.config.config import config
from src.logging_utils.logger import logger
from src.batch_processing.clean_data import clean_data
from experiments.config.optimization_experiment_config import optimization_config


def run_silver_layer_experiment(
    optimization_config: dict[str, Any],
    experiment_name: str,
    bronze_path: str = optimization_config.get_paths_config().get(
        "bronze_layer_dir", "experiments/data/bronze_layer/"
    ),
    silver_path: str = optimization_config.get_paths_config().get(
        "silver_layer_dir", "experiments/data/silver_layer/"
    ),
) -> dict[str, Any]:
    """
    Run silver layer transformations with specific optimization configuration

    Args:
        optimization_config (dict[str, Any]): Dictionary with optimization settings
        experiment_name (str): Name of the experiment for logging
        bronze_path (str): Path to bronze layer data
        silver_path (str): Path to save silver layer data

    Returns:
        Dictionary with performance metrics
    """
    metrics = {
        "experiment_name": experiment_name,
        "stage": "silver_layer",
        "start_time": time.time(),
    }

    # Extract optimization settings
    opts = optimization_config.get("optimizations", {})
    shuffle_partitions = opts.get("shuffle_partitions", 200)
    enable_aqe = opts.get("enable_aqe", False)
    enable_caching = opts.get("enable_caching", False)
    enable_coalesce = opts.get("enable_coalesce", False)
    compression = opts.get("compression", "none")
    broadcast_threshold = opts.get("broadcast_threshold", 10485760)
    enable_partitioning = opts.get("enable_partitioning", False)
    partition_column = opts.get("partition_column", "category")

    logger.info(f"Starting silver layer experiment: {experiment_name}")
    logger.info(f"Optimizations: {opts}")

    # Build Spark session with optimization config
    spark_builder = (
        SparkSession.builder.appName(f"SilverLayer-{experiment_name}")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(enable_aqe).lower())
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled", str(enable_aqe).lower()
        )
        .config("spark.sql.adaptive.skewJoin.enabled", str(enable_aqe).lower())
        .config("spark.sql.autoBroadcastJoinThreshold", str(broadcast_threshold))
    )

    spark = spark_builder.getOrCreate()

    ingestion_order = config.required_tables.get("creation_order", [])
    cleaned_dfs = {}

    try:
        for table in ingestion_order:
            logger.info(f"Processing table: {table}")

            # Read CSV
            input_path = os.path.join(bronze_path, f"{table}.csv")
            df = spark.read.csv(input_path, header=True, inferSchema=True, dateFormat="yyyy-MM-dd")

            # Start timing transformations (DAG building + execution)
            table_transform_start = time.time()

            # Clean the data (lazy)
            df = clean_data(df, table)

            # Apply referential integrity for transactions (lazy)
            if table == "transactions":
                if "users" in cleaned_dfs and "products" in cleaned_dfs:
                    valid_user_ids = cleaned_dfs["users"].select("user_id").distinct()
                    valid_product_ids = (
                        cleaned_dfs["products"].select("product_id").distinct()
                    )

                    df = df.join(valid_user_ids, on="user_id", how="inner")
                    df = df.join(valid_product_ids, on="product_id", how="inner")

                    logger.info("Applied referential integrity filters")

            # Apply caching if enabled (before executing)
            if enable_caching and table in ["users", "products"]:
                df.cache()
                logger.info(f"Caching enabled for table: {table}")

            # Trigger execution with count() to measure actual computation time
            row_count = df.count()

            # Stop timing transformation (DAG + execution via count)
            table_transform_end = time.time()
            table_transform_time = table_transform_end - table_transform_start

            if "table_transform_times" not in metrics:
                metrics["table_transform_times"] = {}
            metrics["table_transform_times"][table] = table_transform_time

            if "table_row_counts" not in metrics:
                metrics["table_row_counts"] = {}
            metrics["table_row_counts"][table] = row_count

            logger.info(
                f"{table} transformation time: {table_transform_time:.2f}s ({row_count} rows)"
            )

            # Store for referential integrity checks
            cleaned_dfs[table] = df

            # Prepare output path
            output_path = os.path.join(silver_path, experiment_name, table)
            if os.path.exists(output_path):
                shutil.rmtree(output_path)

            # Write with optional compression and partitioning (Separate from transformation timing)
            write_start = time.time()

            write_builder = df.write

            if compression != "none":
                write_builder = write_builder.option("compression", compression)

            # Apply partitioning for products table if enabled
            if (
                enable_partitioning
                and table == "products"
                and partition_column in df.columns
            ):
                write_builder = write_builder.partitionBy(partition_column)
                logger.info(f"Partitioning {table} by {partition_column}")

            write_builder.parquet(output_path, mode="overwrite")

            write_time = time.time() - write_start

            if "table_write_times" not in metrics:
                metrics["table_write_times"] = {}
            metrics["table_write_times"][table] = write_time

            logger.info(
                f"Saved {table} to {output_path} (write time: {write_time:.2f}s)"
            )

        # Calculate total metrics
        metrics["transformation_time"] = sum(metrics["table_transform_times"].values())
        metrics["write_time"] = sum(metrics["table_write_times"].values())
        metrics["end_time"] = time.time()
        metrics["total_execution_time"] = metrics["end_time"] - metrics["start_time"]

        logger.info(
            f"Silver layer experiment '{experiment_name}' transformation time: {metrics['transformation_time']:.2f}s"
        )
        logger.info(
            f"Silver layer experiment '{experiment_name}' write time: {metrics['write_time']:.2f}s"
        )
        logger.info(
            f"Silver layer experiment '{experiment_name}' total time: {metrics['total_execution_time']:.2f}s"
        )

    except Exception as e:
        logger.error(f"Silver layer experiment failed: {e}")
        metrics["error"] = str(e)
        raise

    finally:
        # Cleanup caching
        if enable_caching:
            for table in ["users", "products"]:
                df = cleaned_dfs.get(table)
                if df is not None:
                    try:
                        df.unpersist()
                        logger.info(f"Unpersisted {table}")
                    except Exception:
                        pass

        if spark:
            spark.stop()
            logger.info("Spark session stopped")

    return metrics
