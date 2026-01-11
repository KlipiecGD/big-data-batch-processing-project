import pytest
import os
import pandas as pd

from unittest.mock import patch
from pyspark.sql import SparkSession
from src.data_generation.generate_data import generate_transactions_dataset
from src.config.config import config

def test_config_tables_loading():
    """
    Test to ensure that the required tables are loaded from the config.
    """
    required_tables = config.database.get("required_tables", [])
    assert isinstance(required_tables, list), "Required tables should be a list."
    assert len(required_tables) > 0, "There should be at least one required table."
    assert "users" in required_tables, "'users' table should be in the required tables."
    assert "transactions" in required_tables, "'transactions' table should be in the required tables."
    assert "products" in required_tables, "'products' table should be in the required tables."

def test_data_generation(tmp_path):
    """
    Test the data generation function to ensure it creates the expected files.
    """
    test_output_dir = tmp_path / "test_data"
    test_output_dir.mkdir()

    with patch('src.data_generation.generate_data.data_path', str(test_output_dir)):
        generate_transactions_dataset(users_count=5, products_count=2, transactions_count=10) 
        
        # Verify file creation
        assert (test_output_dir / "users.csv").exists()
        assert (test_output_dir / "transactions.csv").exists()
        assert (test_output_dir / "products.csv").exists()
        
        # Verify schemas
        df = pd.read_csv(test_output_dir / "users.csv")
        assert "user_id" in df.columns
        assert "name" in df.columns
        assert "email" in df.columns
        assert "country" in df.columns
        assert "address" in df.columns
        assert "signup_date" in df.columns

        df = pd.read_csv(test_output_dir / "products.csv")
        assert "product_id" in df.columns
        assert "name" in df.columns
        assert "category" in df.columns
        assert "description" in df.columns
        assert "price" in df.columns

        df = pd.read_csv(test_output_dir / "transactions.csv")
        assert "transaction_id" in df.columns
        assert "user_id" in df.columns
        assert "product_id" in df.columns
        assert "quantity" in df.columns
        assert "transaction_date" in df.columns
