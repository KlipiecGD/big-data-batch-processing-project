from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, BooleanType

# Define schema for users table
USERS_SCHEMA = StructType([
    StructField("user_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("address", StringType(), nullable=True),
    StructField("signup_date", TimestampType(), nullable=True)
])

# Define schema for products table
PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("price", FloatType(), nullable=True)
])

# Define schema for transactions table
TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", IntegerType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("transaction_date", TimestampType(), nullable=True)
])