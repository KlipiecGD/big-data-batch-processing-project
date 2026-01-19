import os
import time
from typing import Any
from pyspark.sql import SparkSession
from src.config.config import config
from src.logging_utils.logger import logger
from src.schemas.schemas import USERS_SCHEMA, PRODUCTS_SCHEMA, TRANSACTIONS_SCHEMA
from experiments.config.optimization_experiment_config import optimization_config


def run_gold_layer_experiment(
    optimization_config: dict[str, Any],
    experiment_name: str,
    silver_path: str = optimization_config.get_paths_config().get("silver_layer_dir", "experiments/data/silver_layer"),
    gold_path: str = optimization_config.get_paths_config().get("gold_layer_dir", "experiments/data/gold_layer")
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
        'experiment_name': experiment_name,
        'stage': 'gold_layer',
        'start_time': time.time()
    }
    
    # Extract optimization settings
    opts = optimization_config.get('optimizations', {})
    shuffle_partitions = opts.get('shuffle_partitions', 200)
    enable_aqe = opts.get('enable_aqe', False)
    enable_caching = opts.get('enable_caching', False)
    enable_coalesce = opts.get('enable_coalesce', False)
    broadcast_threshold = opts.get('broadcast_threshold', 10485760)
    enable_partitioning = opts.get('enable_partitioning', False)
    
    logger.info(f"Starting gold layer experiment: {experiment_name}")
    logger.info(f"Optimizations: {opts}")
    
    # Build Spark session with optimization config
    spark_builder = (
        SparkSession.builder
        .appName(f"GoldLayer-{experiment_name}")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(enable_aqe).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", str(enable_aqe).lower())
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
        transactions = spark.read.schema(TRANSACTIONS_SCHEMA).parquet(os.path.join(experiment_silver_path, "transactions"))
        if enable_caching:
            transactions.cache()
            cached_dfs['transactions'] = transactions
        transactions.createOrReplaceTempView("transactions")
        
        # Load users
        users = spark.read.schema(USERS_SCHEMA).parquet(os.path.join(experiment_silver_path, "users"))
        if enable_caching:
            users.cache()
            cached_dfs['users'] = users
        users.createOrReplaceTempView("users")
        
        # Load products
        products = spark.read.schema(PRODUCTS_SCHEMA).parquet(os.path.join(experiment_silver_path, "products"))
        if enable_caching:
            products.cache()
            cached_dfs['products'] = products
        products.createOrReplaceTempView("products")
        
        # If partitioning is enabled, log partition info
        if enable_partitioning:
            logger.info(f"Products table loaded (partitioned by category for pruning)")
        
        logger.info("Silver layer data loaded successfully")
        
        # Start timing transformations
        transformation_start_time = time.time()
        
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
                
                with open(query_path, 'r') as f:
                    query_sql = f.read()
                
                report_name = query_file.split(".")[0]
                
                # Start timing query transformation
                query_start = time.time()
                
                # Execute transformation
                result_df = spark.sql(query_sql)
                
                # Apply coalesce if enabled
                if enable_coalesce:
                    result_df = result_df.coalesce(1)

                # Cache the result so count() and write() share the same execution - avoid double computation
                # We always cache here because count is called below to measure transformation time
                result_df.cache()
                
                # Trigger action to measure pure transformation time (lazy evaluation)
                result_count = result_df.count()

                # Stop timing query transformation
                query_time = time.time() - query_start
                
                query_metrics.append({
                    'query': query_file,
                    'transformation_time': query_time,
                    'result_count': result_count
                })
                
                logger.info(f"Completed {report_name} transformation in {query_time:.2f}s ({result_count} rows)")
                
                # Save to parquet
                output_path = os.path.join(gold_path, experiment_name, report_name)
                result_df.write.parquet(output_path, mode="overwrite")

                result_df.unpersist()
                
            except Exception as e:
                logger.error(f"Failed to process {query_file}: {e}")
                raise
        
        # Transformation time is the sum of individual query times to strictly exclude saving I/O and building spark session
        metrics['transformation_time'] = sum(q['transformation_time'] for q in query_metrics)
        
        # Collect overall metrics 
        metrics['end_time'] = time.time()
        metrics['total_execution_time'] = metrics['end_time'] - metrics['start_time']  # includes building spark session and I/O
        metrics['query_metrics'] = query_metrics
        
        logger.info(f"Gold layer experiment '{experiment_name}' transformation time: {metrics['transformation_time']:.2f}s")
        logger.info(f"Gold layer experiment '{experiment_name}' total time (with building spark session and I/O): {metrics['total_execution_time']:.2f}s")
        
    except Exception as e:
        logger.error(f"Gold layer experiment failed: {e}")
        metrics['error'] = str(e)
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