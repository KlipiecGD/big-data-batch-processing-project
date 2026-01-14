import os
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.config.config import config
from src.logging_utils.logger import logger

load_dotenv()

def create_gold_schema_and_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection) -> None:
    """
    Create gold schema and tables in PostgreSQL based on SQL script.
    
    Args:
        cur: psycopg2 cursor
        conn: psycopg2 connection
    """
    try:
        gold_schema_script_path = config.database.get("gold_layer_tables_schema", "gold_layer/create_tables.sql")
        
        if not os.path.exists(gold_schema_script_path):
            logger.error(f"Gold schema SQL script not found: {gold_schema_script_path}")
            raise FileNotFoundError(f"Missing gold schema script: {gold_schema_script_path}")
        
        with open(gold_schema_script_path, "r") as f:
            sql_script = f.read()
        
        # Execute the SQL script to create schema and tables if not exist
        logger.info("Creating gold schema and tables...")
        cur.execute(sql_script)
        conn.commit()
        logger.info("Gold schema and tables created successfully")
        
    except Exception as e:
        logger.error(f"Failed to create gold schema and tables: {e}")
        if conn:
            conn.rollback()
        raise


def run_gold_layer_creation() -> None:
    """
    Transform silver layer parquet files into gold layer analytical tables in PostgreSQL.
    
    Architecture:
    - Read cleaned data from silver layer Parquet files
    - Perform Spark SQL transformations (aggregations, joins, window functions)
    - Write results to PostgreSQL gold schema tables
    """
    spark = None
    conn = None
    cur = None
    
    try:
        logger.info("Starting gold layer transformation...")
        
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("GoldLayerCreation") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate() # Optimize for smaller datasets
        
        # JDBC Configuration
        db_url = f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        db_props = {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "driver": "org.postgresql.Driver"
        }
        
        # Connect to PostgreSQL for schema/table management
        logger.info("Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        cur = conn.cursor()
        
        # Create gold schema and table definitions
        logger.info("Creating gold schema and table definitions...")
        create_gold_schema_and_tables(cur, conn)
        logger.info("Gold schema and tables ready")
        
        # Define silver layer path
        silver_path = config.data_generation.get("silver_layer_path", "silver_layer/")
        
        # Load data from Parquet files
        logger.info("Loading data from silver layer Parquet files...")
        
        # Load transactions (large fact table)
        transactions = spark.read.parquet(os.path.join(silver_path, "transactions"))
        # Repartition for performance
        # For ~18,000 records, 4 partitions is sufficient
        transactions = transactions.repartition(4)
        transactions.cache() # Cache for performance
        transactions.createOrReplaceTempView("transactions")
        logger.info(f"Transactions loaded: {transactions.count()} records")
        
        # Load users (dimension table)
        users = spark.read.parquet(os.path.join(silver_path, "users"))
        users.cache() # Cache for performance
        users.createOrReplaceTempView("users")
        logger.info(f"Users loaded: {users.count()} records")
        
        # Load products (dimension table)
        products = spark.read.parquet(os.path.join(silver_path, "products"))
        products.cache() # Cache for performance
        products.createOrReplaceTempView("products")
        logger.info(f"Products loaded: {products.count()} records")
        
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
                
                report_name = query_file.split('.')[0]
                gold_table = f"gold.{report_name}"
                
                # Truncate existing data (preserve table schema)
                logger.info(f"Truncating table {gold_table}...")
                cur.execute(f"TRUNCATE TABLE {gold_table};")
                conn.commit()
                
                # Execute Spark SQL transformation
                logger.info(f"Executing Spark SQL transformation for {report_name}...")
                result_df = spark.sql(query_sql)
                
                # Result tables are smaller - use coalesce to reduce partitions
                result_df = result_df.coalesce(1)
                
                # Append to gold table in PostgreSQL (table already exists with schema)
                logger.info(f"Writing results to {gold_table}...")
                result_df.write.jdbc(
                    url=db_url,
                    table=gold_table,
                    mode="append",  # Append mode to preserve schema
                    properties=db_props
                )
                
                logger.info(f"Successfully created gold table {gold_table}")
                
            except Exception as e:
                logger.error(f"Failed to process {query_file}: {e}")
                if conn:
                    conn.rollback()
                raise
        
        logger.info("Gold layer transformation completed successfully")
        logger.info("Gold tables are ready for analytical queries")
        
    except Exception as e:
        logger.error(f"Gold layer transformation failed: {e}")
        raise
        
    finally:
        # Clean up resources
        if spark:
            try:
                transactions.unpersist()
                users.unpersist()
                products.unpersist()
                logger.info("Spark dataframes unpersisted")
            except:
                pass
            try:
                spark.stop()
                logger.info("Spark session stopped")
            except:
                pass
        
        if cur:
            cur.close()
        
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    run_gold_layer_creation()