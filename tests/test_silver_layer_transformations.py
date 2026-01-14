import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
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
def test_parquet_path(tmp_path):
    """
    Fixture to provide a temporary path for Parquet files.
    pytest's tmp_path automatically cleans up after test.
    """
    parquet_dir = tmp_path / "test_silver"
    parquet_dir.mkdir()
    return str(parquet_dir)


def test_clean_users_removes_null_ids(spark_session):
    """
    Test that clean_data removes rows with null user_id.
    """
    # Create test data with null user_id
    data = [
        (1, "John Doe", "john@example.com", "USA", "123 Main St", "2024-01-01"),
        (None, "Jane Smith", "jane@example.com", "Canada", "456 Oak Ave", "2024-01-02"),
        (3, "Bob Johnson", "bob@example.com", "UK", "789 Elm St", "2024-01-03"),
    ]
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("address", StringType(), True),
        StructField("signup_date", StringType(), True),
    ])
    
    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "users")
    
    # Should only have 2 rows (null user_id removed)
    assert cleaned_df.count() == 2
    # Verify no null user_ids remain
    assert cleaned_df.filter("user_id IS NULL").count() == 0


def test_clean_users_removes_invalid_ids(spark_session):
    """
    Test that clean_data removes rows with invalid user_id (≤ 0).
    """
    data = [
        (1, "John Doe", "john@example.com", "USA", "123 Main St", "2024-01-01"),
        (0, "Jane Smith", "jane@example.com", "Canada", "456 Oak Ave", "2024-01-02"),
        (-1, "Bob Johnson", "bob@example.com", "UK", "789 Elm St", "2024-01-03"),
        (2, "Alice Brown", "alice@example.com", "Germany", "321 Pine St", "2024-01-04"),
    ]
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("address", StringType(), True),
        StructField("signup_date", StringType(), True),
    ])
    
    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "users")
    
    # Should only have 2 rows (invalid IDs removed)
    assert cleaned_df.count() == 2
    # Verify all remaining IDs are positive
    assert cleaned_df.filter("user_id <= 0").count() == 0


def test_clean_users_removes_invalid_emails(spark_session):
    """
    Test that clean_data removes rows with invalid emails.
    """
    data = [
        (1, "John Doe", "john@example.com", "USA", "123 Main St", "2024-01-01"),
        (2, "Jane Smith", "jane_at_example.com", "Canada", "456 Oak Ave", "2024-01-02"),  # Corrupted
        (3, "Bob Johnson", "", "UK", "789 Elm St", "2024-01-03"),  # Empty
        (4, "Alice Brown", None, "Germany", "321 Pine St", "2024-01-04"),  # Null
        (5, "Charlie Wilson", "charlie@test.com", "France", "654 Maple Dr", "2024-01-05"),
    ]
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("address", StringType(), True),
        StructField("signup_date", StringType(), True),
    ])
    
    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "users")
    
    # Should only have 2 rows with valid emails
    assert cleaned_df.count() == 2
    # Verify all remaining emails contain @
    assert cleaned_df.filter("email NOT LIKE '%@%'").count() == 0


def test_clean_users_removes_invalid_dates(spark_session):
    """
    Test that clean_data removes rows with invalid signup dates.
    """
    data = [
        (1, "John Doe", "john@example.com", "USA", "123 Main St", "2024-01-01"),
        (2, "Jane Smith", "jane@example.com", "Canada", "456 Oak Ave", "1950-01-01"),  # Too old
        (3, "Bob Johnson", "bob@example.com", "UK", "789 Elm St", "invalid-date"),  # Invalid format
        (4, "Alice Brown", "alice@example.com", "Germany", "321 Pine St", "2023-06-15"),
    ]
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("address", StringType(), True),
        StructField("signup_date", StringType(), True),
    ])
    
    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "users")
    
    # Should only have 2 rows with valid dates
    assert cleaned_df.count() == 2


def test_clean_products_removes_invalid_prices(spark_session):
    """
    Test that clean_data removes rows with invalid prices.
    """
    data = [
        (1, "Product A", "Category 1", "Description A", 10.0),
        (2, "Product B", "Category 2", "Description B", 0.0),  # Invalid
        (3, "Product C", "Category 1", "Description C", -1.0),  # Invalid
        (4, "Product D", "Category 3", "Description D", None),  # Null
        (5, "Product E", "Category 2", "Description E", 25.5),
    ]
    schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), True),
    ])
    
    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "products")
    
    # Should only have 2 rows with valid prices
    assert cleaned_df.count() == 2
    # Verify all remaining prices are positive
    assert cleaned_df.filter("price <= 0").count() == 0


def test_clean_products_removes_empty_fields(spark_session):
    """
    Test that clean_data removes rows with empty name, category, or description.
    """
    data = [
        (1, "Product A", "Category 1", "Description A", 10.0),
        (2, "", "Category 2", "Description B", 20.0),  # Empty name
        (3, "Product C", "", "Description C", 30.0),  # Empty category
        (4, "Product D", "Category 3", "", 40.0),  # Empty description
        (5, "Product E", None, "Description E", 50.0),  # Null category
        (6, "Product F", "Category 2", "Description F", 60.0),
    ]
    schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), True),
    ])
    
    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "products")
    
    # Should only have 2 rows with all fields valid
    assert cleaned_df.count() == 2


def test_clean_transactions_removes_invalid_quantities(spark_session):
    """
    Test that clean_data removes rows with invalid quantities.
    """
    data = [
        (1, 1, 1, 5, "2024-01-01"),
        (2, 2, 2, 0, "2024-01-02"),  # Invalid
        (3, 3, 3, -1, "2024-01-03"),  # Invalid
        (4, 4, 4, None, "2024-01-04"),  # Null
        (5, 5, 5, 3, "2024-01-05"),
    ]
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
    ])
    
    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "transactions")
    
    # Should only have 2 rows with valid quantities
    assert cleaned_df.count() == 2
    # Verify all remaining quantities are positive
    assert cleaned_df.filter("quantity <= 0").count() == 0


def test_clean_transactions_removes_invalid_dates(spark_session):
    """
    Test that clean_data removes rows with invalid transaction dates.
    """
    data = [
        (1, 1, 1, 5, "2024-01-01"),
        (2, 2, 2, 3, "1950-01-01"),  # Too old
        (3, 3, 3, 2, "invalid-date"),  # Invalid format
        (4, 4, 4, 4, "2023-06-15"),
    ]
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
    ])
    
    df = spark_session.createDataFrame(data, schema)
    cleaned_df = clean_data(df, "transactions")
    
    # Should only have 2 rows with valid dates
    assert cleaned_df.count() == 2


def test_parquet_write_and_read(spark_session, test_parquet_path):
    """
    Test that data can be written to and read from Parquet format.
    """
    # Create test data
    data = [
        (1, "John Doe", "john@example.com", "USA", "123 Main St", "2024-01-01"),
        (2, "Jane Smith", "jane@example.com", "Canada", "456 Oak Ave", "2024-01-02"),
    ]
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("address", StringType(), True),
        StructField("signup_date", StringType(), True),
    ])
    
    df = spark_session.createDataFrame(data, schema)
    
    # Write to Parquet
    parquet_file = os.path.join(test_parquet_path, "users")
    df.write.parquet(parquet_file, mode="overwrite")
    
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


def test_parquet_schema_preservation(spark_session, test_parquet_path):
    """
    Test that Parquet format preserves data types.
    """
    data = [
        (1, "Product A", "Category 1", "Description A", 10.99),
        (2, "Product B", "Category 2", "Description B", 25.50),
    ]
    schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), True),
    ])
    
    df = spark_session.createDataFrame(data, schema)
    
    # Write to Parquet
    parquet_file = os.path.join(test_parquet_path, "products")
    df.write.parquet(parquet_file, mode="overwrite")
    
    # Read back and verify schema
    df_read = spark_session.read.parquet(parquet_file)
    
    assert df_read.schema == df.schema
    assert df_read.schema["product_id"].dataType == IntegerType()
    assert df_read.schema["price"].dataType == DoubleType()


def test_clean_data_referential_integrity_simulation(spark_session):
    """
    Test the concept of referential integrity by ensuring transactions
    only reference valid user and product IDs.
    """
    # Create users
    users_data = [(1,), (2,), (3,)]
    users_schema = StructType([StructField("user_id", IntegerType(), True)])
    users_df = spark_session.createDataFrame(users_data, users_schema)
    
    # Create products
    products_data = [(10,), (20,), (30,)]
    products_schema = StructType([StructField("product_id", IntegerType(), True)])
    products_df = spark_session.createDataFrame(products_data, products_schema)
    
    # Create transactions (some with invalid foreign keys)
    transactions_data = [
        (1, 1, 10, 5, "2024-01-01"),  # Valid
        (2, 2, 20, 3, "2024-01-02"),  # Valid
        (3, 99, 30, 2, "2024-01-03"),  # Invalid user_id
        (4, 3, 99, 4, "2024-01-04"),  # Invalid product_id
        (5, 3, 30, 1, "2024-01-05"),  # Valid
    ]
    transactions_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
    ])
    transactions_df = spark_session.createDataFrame(transactions_data, transactions_schema)
    
    # Apply referential integrity filter (simulating what happens in process_silver_layer)
    valid_user_ids = users_df.select("user_id").distinct()
    valid_product_ids = products_df.select("product_id").distinct()
    
    filtered_df = transactions_df.join(valid_user_ids, on="user_id", how="inner")
    filtered_df = filtered_df.join(valid_product_ids, on="product_id", how="inner")
    
    # Should only have 3 valid transactions
    assert filtered_df.count() == 3
    
    # Verify no invalid foreign keys remain
    user_ids = [row.user_id for row in filtered_df.select("user_id").distinct().collect()]
    assert all(uid in [1, 2, 3] for uid in user_ids)
    
    product_ids = [row.product_id for row in filtered_df.select("product_id").distinct().collect()]
    assert all(pid in [10, 20, 30] for pid in product_ids)