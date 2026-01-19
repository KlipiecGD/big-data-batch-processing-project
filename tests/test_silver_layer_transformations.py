import pytest
import os
from pyspark.sql import SparkSession

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

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


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """
    Fixture to create a Spark session for testing.
    """
    return (
        SparkSession.builder.master("local[1]")
        .appName("SilverLayerTests")
        .getOrCreate()
    )


@pytest.fixture
def test_parquet_path(tmp_path) -> str:
    """
    Fixture to provide a temporary path for Parquet files.
    pytest's tmp_path automatically cleans up after test.
    """
    parquet_dir = tmp_path / "test_silver"
    parquet_dir.mkdir()
    return str(parquet_dir)


def test_clean_users_removes_null_ids(spark_session) -> None:
    """
    Test that clean_data removes rows with null user_id.
    """
    # Create test data with null user_id
    data = USERS_NULL_IDS
    schema = USERS_SCHEMA

    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "users")

    # Should only have 2 rows (null user_id removed)
    assert cleaned_df.count() == 2
    # Verify no null user_ids remain
    assert cleaned_df.filter("user_id IS NULL").count() == 0


def test_clean_users_removes_invalid_ids(spark_session) -> None:
    """
    Test that clean_data removes rows with invalid user_id (≤ 0).
    """
    data = USERS_INVALID_IDS
    schema = USERS_SCHEMA

    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "users")

    # Should only have 2 rows (invalid IDs removed)
    assert cleaned_df.count() == 2
    # Verify all remaining IDs are positive
    assert cleaned_df.filter("user_id <= 0").count() == 0


def test_clean_users_removes_invalid_emails(spark_session) -> None:
    """
    Test that clean_data removes rows with invalid emails.
    """
    data = USERS_INVALID_EMAILS
    schema = USERS_SCHEMA

    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "users")

    # Should only have 2 rows with valid emails
    assert cleaned_df.count() == 2
    # Verify all remaining emails contain @
    assert cleaned_df.filter("email NOT LIKE '%@%'").count() == 0


def test_clean_users_removes_invalid_dates(spark_session) -> None:
    """
    Test that clean_data removes rows with invalid signup dates.
    """
    data = USERS_INVALID_DATES
    schema = USERS_SCHEMA

    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "users")

    # Should only have 2 rows with valid dates
    assert cleaned_df.count() == 2


def test_clean_products_removes_invalid_prices(spark_session) -> None:
    """
    Test that clean_data removes rows with invalid prices.
    """
    data = PRODUCTS_INVALID_PRICES
    schema = PRODUCTS_SCHEMA

    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "products")

    # Should only have 2 rows with valid prices
    assert cleaned_df.count() == 2
    # Verify all remaining prices are positive
    assert cleaned_df.filter("price <= 0").count() == 0


def test_clean_products_removes_empty_fields(spark_session) -> None:
    """
    Test that clean_data removes rows with empty name, category, or description.
    """
    data = PRODUCTS_EMPTY_FIELDS
    schema = PRODUCTS_SCHEMA

    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "products")

    # Should only have 2 rows with all fields valid
    assert cleaned_df.count() == 2


def test_clean_transactions_removes_invalid_quantities(spark_session) -> None:
    """
    Test that clean_data removes rows with invalid quantities.
    """
    data = TRANSACTIONS_INVALID_QUANTS
    schema = TRANSACTIONS_SCHEMA

    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "transactions")

    # Should only have 2 rows with valid quantities
    assert cleaned_df.count() == 2
    # Verify all remaining quantities are positive
    assert cleaned_df.filter("quantity <= 0").count() == 0


def test_clean_transactions_removes_invalid_dates(spark_session) -> None:
    """
    Test that clean_data removes rows with invalid transaction dates.
    """
    data = TRANSACTIONS_INVALID_DATES
    schema = TRANSACTIONS_SCHEMA

    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "transactions")

    # Should only have 2 rows with valid dates
    assert cleaned_df.count() == 2


def test_parquet_write_and_read(spark_session, test_parquet_path) -> None:
    """
    Test that data can be written to and read from Parquet format.
    """
    # Create test data
    data = USERS_VALID
    schema = USERS_SCHEMA

    df = spark_session.createDataFrame(data, schema)

    # Write to Parquet
    parquet_file = os.path.join(test_parquet_path, "users")
    df.write.parquet(parquet_file, mode="overwrite", compression="snappy")

    # Verify file was created
    assert os.path.exists(parquet_file)

    # Read back from Parquet
    df_read = spark_session.read.parquet(parquet_file)

    # Verify data integrity
    assert df_read.count() == 2
    assert set(df_read.columns) == set(df.columns)

    # Verify content matches
    original_data = df.collect()
    read_data = df_read.collect()
    assert len(original_data) == len(read_data)


def test_parquet_schema_preservation(spark_session, test_parquet_path) -> None:
    """
    Test that Parquet format preserves data types.
    """
    data = PRODUCTS_VALID
    schema = PRODUCTS_SCHEMA

    df = spark_session.createDataFrame(data, schema)

    # Write to Parquet
    parquet_file = os.path.join(test_parquet_path, "products")
    df.write.parquet(parquet_file, mode="overwrite", compression="snappy")

    # Read back and verify schema
    df_read = spark_session.read.parquet(parquet_file)

    assert df_read.schema == df.schema
    assert df_read.schema["product_id"].dataType == IntegerType()
    assert df_read.schema["price"].dataType == DoubleType()


def test_clean_data_referential_integrity_simulation(spark_session) -> None:
    """
    Test the concept of referential integrity by ensuring transactions
    only reference valid user and product IDs.
    """
    # Create users
    users_data = USERS_REFERENTIAL
    users_schema = USERS_SCHEMA
    users_df = spark_session.createDataFrame(users_data, users_schema)

    # Create products
    products_data = PRODUCTS_REFERENTIAL
    products_schema = PRODUCTS_SCHEMA
    products_df = spark_session.createDataFrame(products_data, products_schema)

    # Create transactions (some with invalid foreign keys)
    transactions_data = TRANSACTIONS_REFERENTIAL
    transactions_schema = TRANSACTIONS_SCHEMA
    transactions_df = spark_session.createDataFrame(
        transactions_data, transactions_schema
    )

    # Apply referential integrity filter (simulating what happens in process_silver_layer)
    valid_user_ids = users_df.select("user_id").distinct()
    valid_product_ids = products_df.select("product_id").distinct()

    filtered_df = transactions_df.join(valid_user_ids, on="user_id", how="inner")
    filtered_df = filtered_df.join(valid_product_ids, on="product_id", how="inner")

    # Should only have 3 valid transactions
    assert filtered_df.count() == 3

    # Verify no invalid foreign keys remain
    user_ids = [
        row.user_id for row in filtered_df.select("user_id").distinct().collect()
    ]
    assert all(uid in [1, 2, 3] for uid in user_ids)

    product_ids = [
        row.product_id for row in filtered_df.select("product_id").distinct().collect()
    ]
    assert all(pid in [10, 20, 30] for pid in product_ids)
