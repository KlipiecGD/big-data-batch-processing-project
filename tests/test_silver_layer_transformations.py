import pytest
import os
from typing import Generator
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType

from tests.data.test_data_silver import (
    USERS_SCHEMA,
    USERS_NULL_IDS,
    USERS_INVALID_IDS,
    USERS_INVALID_EMAILS,
    USERS_INVALID_DATES,
    USERS_VALID,
    USERS_REFERENTIAL,
    PRODUCTS_SCHEMA,
    PRODUCTS_INVALID_PRICES,
    PRODUCTS_EMPTY_FIELDS,
    PRODUCTS_VALID,
    PRODUCTS_REFERENTIAL,
    TRANSACTIONS_SCHEMA,
    TRANSACTIONS_INVALID_QUANTS,
    TRANSACTIONS_INVALID_DATES,
    TRANSACTIONS_REFERENTIAL,
)
from src.batch_processing.clean_data import clean_data


@pytest.fixture(scope="module")
def spark() -> Generator[SparkSession, None, None]:
    """Create a Spark session for testing."""
    spark_session = (
        SparkSession.builder.appName("TestSilverTransformations")
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture
def test_parquet_path(tmp_path) -> str:
    """Fixture to provide a temporary path for Parquet files."""
    parquet_dir = tmp_path / "test_silver"
    parquet_dir.mkdir()
    return str(parquet_dir)


class TestUsersDataCleaning:
    """Test suite for users table data cleaning."""

    def test_removes_null_ids(self, spark: SparkSession):
        """Test that rows with null user_id are removed."""
        df = spark.createDataFrame(USERS_NULL_IDS, schema=USERS_SCHEMA)
        initial_count = df.count()
        null_count = df.filter("user_id IS NULL").count()

        # Ensure test data has nulls
        assert null_count > 0, "Test data should contain null user_ids"

        cleaned_df = clean_data(df, "users")

        assert cleaned_df.filter("user_id IS NULL").count() == 0, (
            "Cleaned data should not contain null user_ids"
        )
        assert cleaned_df.count() == initial_count - null_count, (
            f"Expected {initial_count - null_count} rows after cleaning"
        )

    def test_removes_invalid_ids(self, spark: SparkSession):
        """Test that rows with invalid user_id (≤ 0) are removed."""
        df = spark.createDataFrame(USERS_INVALID_IDS, schema=USERS_SCHEMA)
        initial_invalid_count = df.filter("user_id <= 0").count()

        # Ensure test data has invalid IDs
        assert initial_invalid_count > 0, "Test data should contain invalid user_ids"

        cleaned_df = clean_data(df, "users")

        assert cleaned_df.filter("user_id <= 0").count() == 0, (
            "Cleaned data should not contain user_id <= 0"
        )

    def test_removes_invalid_emails(self, spark: SparkSession):
        """Test that rows with invalid emails are removed."""
        df = spark.createDataFrame(USERS_INVALID_EMAILS, schema=USERS_SCHEMA)
        initial_invalid_count = df.filter(
            "email IS NULL OR email = '' OR email NOT LIKE '%@%'"
        ).count()

        # Ensure test data has invalid emails
        assert initial_invalid_count > 0, "Test data should contain invalid emails"

        cleaned_df = clean_data(df, "users")

        assert cleaned_df.filter("email IS NULL OR email = ''").count() == 0, (
            "Cleaned data should not contain null or empty emails"
        )
        assert cleaned_df.filter("email NOT LIKE '%@%'").count() == 0, (
            "All emails should contain @ symbol"
        )

    def test_removes_invalid_dates(self, spark: SparkSession):
        """Test that rows with invalid signup dates are removed."""
        df = spark.createDataFrame(USERS_INVALID_DATES, schema=USERS_SCHEMA)
        initial_count = df.count()
        cleaned_df = clean_data(df, "users")
        final_count = cleaned_df.count()

        # Should have fewer rows after cleaning
        assert final_count < initial_count, (
            "Cleaning should remove rows with invalid dates"
        )

    def test_preserves_valid_data(self, spark: SparkSession):
        """Test that valid user records pass through unchanged."""
        df = spark.createDataFrame(USERS_VALID, schema=USERS_SCHEMA)
        cleaned_df = clean_data(df, "users")

        assert cleaned_df.count() == len(USERS_VALID), (
            "All valid records should be preserved"
        )


class TestProductsDataCleaning:
    """Test suite for products table data cleaning."""

    def test_removes_invalid_prices(self, spark: SparkSession):
        """Test that rows with invalid prices are removed."""
        df = spark.createDataFrame(PRODUCTS_INVALID_PRICES, schema=PRODUCTS_SCHEMA)
        initial_invalid_count = df.filter("price IS NULL OR price <= 0").count()

        # Ensure test data has invalid prices
        assert initial_invalid_count > 0, "Test data should contain invalid prices"

        cleaned_df = clean_data(df, "products")

        assert cleaned_df.filter("price IS NULL OR price <= 0").count() == 0, (
            "Cleaned data should not contain invalid prices"
        )

    def test_removes_empty_fields(self, spark: SparkSession):
        """Test that rows with empty required fields are removed."""
        df = spark.createDataFrame(PRODUCTS_EMPTY_FIELDS, schema=PRODUCTS_SCHEMA)

        # Count initial invalid rows
        initial_invalid_count = df.filter(
            "name IS NULL OR name = '' OR "
            "category IS NULL OR category = '' OR "
            "description IS NULL OR description = ''"
        ).count()

        # Ensure test data has empty fields
        assert initial_invalid_count > 0, "Test data should contain empty required fields"

        cleaned_df = clean_data(df, "products")

        # Verify no empty required fields remain
        for field in ["name", "category", "description"]:
            assert cleaned_df.filter(f"{field} IS NULL OR {field} = ''").count() == 0, (
                f"Cleaned data should not contain null or empty {field}"
            )

    def test_preserves_valid_products(self, spark: SparkSession):
        """Test that valid product records pass through unchanged."""
        df = spark.createDataFrame(PRODUCTS_VALID, schema=PRODUCTS_SCHEMA)
        cleaned_df = clean_data(df, "products")

        assert cleaned_df.count() == len(PRODUCTS_VALID), (
            "All valid records should be preserved"
        )


class TestTransactionsDataCleaning:
    """Test suite for transactions table data cleaning."""

    def test_removes_invalid_quantities(self, spark: SparkSession):
        """Test that rows with invalid quantities are removed."""
        df = spark.createDataFrame(
            TRANSACTIONS_INVALID_QUANTS, schema=TRANSACTIONS_SCHEMA
        )
        initial_invalid_count = df.filter("quantity IS NULL OR quantity <= 0").count()

        # Ensure test data has invalid quantities
        assert initial_invalid_count > 0, "Test data should contain invalid quantities"

        cleaned_df = clean_data(df, "transactions")

        assert cleaned_df.filter("quantity IS NULL OR quantity <= 0").count() == 0, (
            "Cleaned data should not contain invalid quantities"
        )

    def test_removes_invalid_dates(self, spark: SparkSession):
        """Test that rows with invalid transaction dates are removed."""
        df = spark.createDataFrame(
            TRANSACTIONS_INVALID_DATES, schema=TRANSACTIONS_SCHEMA
        )
        initial_count = df.count()
        cleaned_df = clean_data(df, "transactions")
        final_count = cleaned_df.count()

        assert final_count < initial_count, (
            "Cleaning should remove rows with invalid dates"
        )


class TestReferentialIntegrity:
    """Test suite for referential integrity enforcement."""

    def test_filters_invalid_foreign_keys(self, spark: SparkSession):
        """Test that transactions with invalid foreign keys are filtered out."""
        # Create dimension tables
        users_df = spark.createDataFrame(USERS_REFERENTIAL, schema=USERS_SCHEMA)
        products_df = spark.createDataFrame(PRODUCTS_REFERENTIAL, schema=PRODUCTS_SCHEMA)
        transactions_df = spark.createDataFrame(
            TRANSACTIONS_REFERENTIAL, schema=TRANSACTIONS_SCHEMA
        )

        valid_user_ids = {row[0] for row in USERS_REFERENTIAL}
        valid_product_ids = {row[0] for row in PRODUCTS_REFERENTIAL}

        # Count transactions with invalid foreign keys
        initial_count = len(TRANSACTIONS_REFERENTIAL)
        valid_count = sum(
            1
            for row in TRANSACTIONS_REFERENTIAL
            if row[1] in valid_user_ids and row[2] in valid_product_ids
        )

        # Ensure test data has invalid foreign keys
        assert valid_count < initial_count, (
            "Test data should contain invalid foreign keys"
        )

        # Apply referential integrity filters
        valid_user_ids_df = users_df.select("user_id").distinct()
        valid_product_ids_df = products_df.select("product_id").distinct()

        filtered_df = transactions_df.join(valid_user_ids_df, on="user_id", how="inner")
        filtered_df = filtered_df.join(
            valid_product_ids_df, on="product_id", how="inner"
        )

        assert filtered_df.count() == valid_count, (
            f"Expected {valid_count} valid transactions after filtering"
        )

        # Verify no invalid foreign keys remain
        result_user_ids = {
            row.user_id for row in filtered_df.select("user_id").distinct().collect()
        }
        result_product_ids = {
            row.product_id
            for row in filtered_df.select("product_id").distinct().collect()
        }

        assert result_user_ids.issubset(valid_user_ids), (
            "All remaining user_ids should be valid"
        )
        assert result_product_ids.issubset(valid_product_ids), (
            "All remaining product_ids should be valid"
        )


class TestParquetOperations:
    """Test suite for Parquet file operations."""

    def test_write_and_read_parquet(self, spark: SparkSession, test_parquet_path):
        """Test that data can be written to and read from Parquet format."""
        df = spark.createDataFrame(USERS_VALID, schema=USERS_SCHEMA)
        parquet_file = os.path.join(test_parquet_path, "users")

        # Write to Parquet
        df.write.parquet(parquet_file, mode="overwrite", compression="snappy")

        # Verify file creation
        assert os.path.exists(parquet_file), "Parquet file should be created"

        # Read back
        df_read = spark.read.parquet(parquet_file)

        # Verify data integrity
        assert df_read.count() == df.count(), "Row count should be preserved"
        assert set(df_read.columns) == set(df.columns), "Columns should be preserved"

    def test_schema_preservation(self, spark: SparkSession, test_parquet_path):
        """Test that Parquet format preserves data types."""
        df = spark.createDataFrame(PRODUCTS_VALID, schema=PRODUCTS_SCHEMA)
        parquet_file = os.path.join(test_parquet_path, "products")

        # Write and read
        df.write.parquet(parquet_file, mode="overwrite", compression="snappy")
        df_read = spark.read.parquet(parquet_file)

        # Verify schema preservation
        assert df_read.schema == df.schema, "Schema should be preserved"
        assert df_read.schema["product_id"].dataType == IntegerType(), (
            "product_id should remain IntegerType"
        )
        assert df_read.schema["price"].dataType == DoubleType(), (
            "price should remain DoubleType"
        )