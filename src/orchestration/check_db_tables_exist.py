from dotenv import load_dotenv

import os
import psycopg2
from psycopg2 import sql

from src.config.config import config
from src.logging_utils.logger import logger

load_dotenv()

def check_db_tables_exist(**kwargs) -> str:
    """
    Check if specific tables exist in the PostgreSQL database.
    If at least one table is missing, it is considered a failure.
    Args:
        **kwargs: Additional keyword arguments.
    Returns:
        str: 'skip_db_setup' if all tables exist, 'create_postgres_tables' otherwise.
    """
    logger.info("Checking database tables existence...")
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST")
        )
        cursor = conn.cursor()
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        return 'create_postgres_tables'
    
    tables_to_check = config.database.get("required_tables", [])

    for table in tables_to_check:
        # Check if the table exists
        cursor.execute(
            sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)"),
            [table]
        )
        result = cursor.fetchone()
        exists = result[0] if result else False

        # If any table does not exist, return 'create_postgres_tables'
        if not exists:
            logger.info(f"Table '{table}' does not exist. Need to create tables.")
            cursor.close()
            conn.close()
            return 'create_postgres_tables'

    cursor.close()
    conn.close()
    return 'skip_db_setup'