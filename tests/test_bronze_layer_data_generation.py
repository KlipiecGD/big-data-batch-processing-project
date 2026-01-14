import pandas as pd

from unittest.mock import patch
from src.data_generation.generate_bronze_layer_data import generate_transactions_dataset

def test_data_generation(tmp_path):
    """
    Test the data generation function to ensure it creates the expected files.
    """
    test_output_dir = tmp_path / "test_data"
    test_output_dir.mkdir()

    with patch("src.data_generation.generate_bronze_layer_data.data_path", str(test_output_dir)):
        generate_transactions_dataset(
            users_count=5, products_count=2, transactions_count=10,
            save_to_cloud=False
        )

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

def test_noise_injection(tmp_path):
    """
    Test to ensure that noise is being injected into the generated data.
    """
    test_output_dir = tmp_path / "test_data_noise"
    test_output_dir.mkdir()

    with patch("src.data_generation.generate_bronze_layer_data.data_path", str(test_output_dir)):
        generate_transactions_dataset(
            users_count=100, products_count=50, transactions_count=200,
            noise_level=0.1, null_wrong_proportion=0.5
        ) # It is almost impossible that no noise is injected with these parameters

        # Load generated data
        df_users = pd.read_csv(test_output_dir / "users.csv")
        df_products = pd.read_csv(test_output_dir / "products.csv")
        df_transactions = pd.read_csv(test_output_dir / "transactions.csv")

        # Check for nulls and incorrect values in users
        assert df_users.isnull().values.any(), "No null values found in users data."
        
        # Check for nulls and incorrect values in products
        assert df_products.isnull().values.any(), "No null values found in products data."
        
        # Check for nulls and incorrect values in transactions
        assert df_transactions.isnull().values.any(), "No null values found in transactions data."