import os
import time
from typing import Any
from pyspark.sql import SparkSession
from src.config.config import config
from src.logging_utils.logger import logger
from experiments.config.optimization_experiment_config import optimization_config


def run_gold_layer_experiment(
    optimization_config: dict[str, Any],
    experiment_name: str,
    silver_path: str = optimization_config.get_paths_config().get(
        "silver_layer_dir", "experiments/data/silver_layer/"
    ),
    gold_path: str = optimization_config.get_paths_config().get(
        "gold_layer_dir", "experiments/data/gold_layer/"
    ),
) -> dict[str, Any]:
    """
    Run gold layer transformations with specific optimization configuration

    Args:
        optimization_config (dict[str, Any]): Dictionary with optimization settings
        experiment_name (str): Name of the experiment for logging
        silver_path (str): Path to silver layer data
        gold_path (str): Path to save gold layer results

    Returns:
        Dictionary with performance metrics
    """
    metrics = {
        "experiment_name": experiment_name,
        "stage": "gold_layer",
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

    logger.info(f"Starting gold layer experiment: {experiment_name}")
    logger.info(f"Optimizations: {opts}")

    # Build Spark session with optimization config
    spark_builder = (
        SparkSession.builder.appName(f"GoldLayer-{experiment_name}")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(enable_aqe).lower())
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled", str(enable_aqe).lower()
        )
        .config("spark.sql.adaptive.skewJoin.enabled", str(enable_aqe).lower())
        .config("spark.sql.autoBroadcastJoinThreshold", str(broadcast_threshold))
    )

    spark = spark_builder.getOrCreate()

    cached_dfs = {}

    try:
        # Load silver layer data
        logger.info("Loading silver layer data...")

        experiment_silver_path = os.path.join(silver_path, experiment_name)

        # Load transactions
        transactions = spark.read.parquet(
            os.path.join(experiment_silver_path, "transactions"), inferSchema=True
        )
        if enable_caching:
            transactions.cache()
            transactions.count()  # Materialize cache
            cached_dfs["transactions"] = transactions
            logger.info("Cached transactions table")
        transactions.createOrReplaceTempView("transactions")

        # Load users
        users = spark.read.parquet(
            os.path.join(experiment_silver_path, "users"), inferSchema=True
        )
        if enable_caching:
            users.cache()
            users.count()  # Materialize cache
            cached_dfs["users"] = users
            logger.info("Cached users table")
        users.createOrReplaceTempView("users")

        # Load products
        products = spark.read.parquet(
            os.path.join(experiment_silver_path, "products"), inferSchema=True
        )
        if enable_caching:
            products.cache()
            products.count()  # Materialize cache
            cached_dfs["products"] = products
            logger.info("Cached products table")
        products.createOrReplaceTempView("products")

        # If partitioning is enabled, log partition info
        if enable_partitioning:
            logger.info("Products table loaded (partitioned by category for pruning)")

        logger.info("Silver layer data loaded successfully")

        # Process each query
        query_files = config.queries.get("query_files", [])
        query_metrics = []

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

                # Start timing query transformation (DAG building + execution)
                query_start = time.time()

                # Execute transformation (lazy)
                result_df = spark.sql(query_sql)

                # Apply coalesce if enabled (lazy)
                if enable_coalesce:
                    result_df = result_df.coalesce(1)

                # Trigger execution with count() to measure actual computation time
                result_count = result_df.count()

                # Stop timing query transformation (DAG + execution via count)
                query_time = time.time() - query_start

                query_metrics.append(
                    {
                        "query": query_file,
                        "transformation_time": query_time,
                        "result_count": result_count,
                    }
                )

                logger.info(
                    f"Completed {report_name} transformation in {query_time:.2f}s ({result_count} rows)"
                )

                # Save to parquet (Separate from transformation timing)
                write_start = time.time()

                output_path = os.path.join(gold_path, experiment_name, report_name)

                write_builder = result_df.write
                if compression != "none":
                    write_builder = write_builder.option("compression", compression)

                write_builder.parquet(output_path, mode="overwrite")

                write_time = time.time() - write_start
                query_metrics[-1]["write_time"] = write_time

                logger.info(f"Saved {report_name} (write time: {write_time:.2f}s)")

            except Exception as e:
                logger.error(f"Failed to process {query_file}: {e}")
                raise

        # Collect overall metrics
        metrics["end_time"] = time.time()
        metrics["transformation_time"] = sum(
            q["transformation_time"] for q in query_metrics
        )
        metrics["write_time"] = sum(q.get("write_time", 0) for q in query_metrics)
        metrics["total_execution_time"] = metrics["end_time"] - metrics["start_time"]
        metrics["query_metrics"] = query_metrics

        logger.info(
            f"Gold layer experiment '{experiment_name}' transformation time: {metrics['transformation_time']:.2f}s"
        )
        logger.info(
            f"Gold layer experiment '{experiment_name}' write time: {metrics['write_time']:.2f}s"
        )
        logger.info(
            f"Gold layer experiment '{experiment_name}' total time: {metrics['total_execution_time']:.2f}s"
        )

    except Exception as e:
        logger.error(f"Gold layer experiment failed: {e}")
        metrics["error"] = str(e)
        raise

    finally:
        # Cleanup caching
        if enable_caching:
            for df_name, df in cached_dfs.items():
                try:
                    df.unpersist()
                    logger.info(f"Unpersisted {df_name}")
                except Exception:
                    pass

        if spark:
            spark.stop()
            logger.info("Spark session stopped")

    return metrics
