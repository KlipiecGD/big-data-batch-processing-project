import os
import psycopg2
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config.config import config
from src.logging_utils.logger import logger

load_dotenv()


def load_query(file_name: str) -> str:
    """
    Load SQL query from a file.

    Args:
        file_name (str): The name of the SQL file.

    Returns:
        str: The SQL query as a string.
    """
    queries_path = config.queries.get("queries_path", "sql_queries/")
    try:
        with open(os.path.join(queries_path, file_name), "r") as file:
            query = file.read()
        return query
    except Exception as e:
        logger.error(f"Error loading SQL query from {file_name}: {e}")
        return ""


def run_batch_processing() -> None:
    """
    Run batch processing tasks using PySpark.
    """
    try:
        logger.info("Starting batch processing...")

        # Initialize Spark session
        spark = (
            SparkSession.builder.appName("BatchProcessing")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
            .getOrCreate()
        )
        logger.info("Spark session initialized.")
    except Exception as e:
        logger.error(f"Error initializing Spark session: {e}")
        return

    # Database connection properties
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "bigdata_db")
    db_user = os.getenv("DB_USER", "user")
    db_password = os.getenv("DB_PASSWORD", "password")

    # JDBC properties
    db_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver",
    }
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    # Set data path from config
    data_path = config.data_generation.get("data_path", "data/")

    # Save source tables to PostgreSQL
    # Get list of source tables from config
    source_tables = config.database.get("required_tables", [])

    # Manually truncate tables with CASCADE to preserve schema
    try:
        logger.info("Manually truncating tables with CASCADE to preserve schema...")

        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        cur = conn.cursor()

        # Truncate each table
        for table in source_tables:
            cur.execute(f"TRUNCATE TABLE {table} CASCADE;")

        # Commit changes and close connection
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Tables truncated successfully.")
    except Exception as e:
        logger.error(f"Failed to truncate tables: {e}")
    finally:
        if "cur" in locals() and not cur.closed:
            cur.close()
        if "conn" in locals() and conn and not conn.closed:
            conn.close()

    for table in source_tables:
        try:
            logger.info(f"Saving source table {table} to PostgreSQL...")

            # Read CSV into DataFrame
            df = spark.read.csv(
                os.path.join(data_path, f"{table}.csv"), header=True, inferSchema=True
            )

            # Create view for further processing
            df.createOrReplaceTempView(table)

            # Write DataFrame to PostgreSQL
            df.write.jdbc(
                url=jdbc_url, table=table, mode="append", properties=db_properties
            )

            logger.info(f"Successfully saved {table} to PostgreSQL.")
        except Exception as e:
            logger.error(f"Error saving source table {table}: {e}")

    query_files_list = config.queries.get("query_files", [])

    # Perform complex analytical queries
    for query_file in query_files_list:
        query = load_query(query_file)
        if query:
            try:
                logger.info(f"Executing and saving query: {query_file}")

                # Execute query and hold result in a DataFrame
                result_df = spark.sql(query)

                # Define table name
                table_name = f"report_{query_file.split('.')[0]}"

                # Write result to PostgreSQL
                result_df.write.jdbc(
                    url=jdbc_url,
                    table=table_name,
                    mode="overwrite",  # Refreshes the analytical report
                    properties=db_properties,
                )

                logger.info(f"Successfully saved {table_name} to PostgreSQL.")
            except Exception as e:
                logger.error(f"Error processing {query_file}: {e}")

    spark.stop()
    logger.info("Batch processing completed.")


if __name__ == "__main__":
    run_batch_processing()
