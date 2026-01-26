from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

# Schema definitions
USERS_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("country", StringType(), False),
        StructField("address", StringType(), False),
        StructField("signup_date", StringType(), False),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("description", StringType(), False),
        StructField("price", DoubleType(), False),
    ]
)

TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("transaction_date", StringType(), False),
    ]
)

# Required fields for gold layer outputs
TOP_SPENDERS_FIELDS = {"user_id", "name", "total_spent"}
TOP_PRODUCTS_FIELDS = {"category", "product_id", "name", "total_quantity_sold"}
COUNTRY_SALES_FIELDS = {"country", "total_sales", "total_transactions", "total_users"}
MOVING_AVERAGE_SALES_FIELDS = {"transaction_date", "total_sales", "moving_average_sales"}
DAILY_SALES_TRENDS_FIELDS = {"transaction_date", "total_sales", "previous_day_sales", "percent_change"}



# Test data for gold layer transformations
USERS_DATA = [
    # USA - 2 users
    (1, "John", "john@example.com", "USA", "123 Main St", "2024-01-01"),
    (2, "Sarah", "sarah@example.com", "USA", "456 Oak Ave", "2024-01-15"),
    # Poland - 3 users
    (3, "Kacper", "kacper@example.com", "Poland", "789 Pine Rd", "2024-02-01"),
    (4, "Maria", "maria@example.com", "Poland", "321 Elm St", "2024-02-10"),
    (5, "Emma", "emma@example.com", "Poland", "654 Maple Dr", "2024-02-20"),
    # Germany - 2 users
    (6, "Anna", "anna@example.com", "Germany", "987 Birch Ln", "2024-03-01"),
    (7, "Peter", "peter@example.com", "Germany", "147 Cedar Ct", "2024-03-10"),
    # France - 1 user
    (8, "Lucas", "lucas@example.com", "France", "258 Spruce Way", "2024-04-01"),
]

# Products data - 10 products across 3 categories
PRODUCTS_DATA = [
    # Category 1 - 4 products
    (1, "Product A", "Category 1", "Description A", 10.0),
    (2, "Product B", "Category 1", "Description B", 5.0),
    (3, "Product C", "Category 1", "Description C", 8.0),
    (9, "Product I", "Category 1", "Description I", 3.0),
    # Category 2 - 4 products
    (4, "Product D", "Category 2", "Description D", 25.0),
    (5, "Product E", "Category 2", "Description E", 12.0),
    (6, "Product F", "Category 2", "Description F", 50.0),
    (10, "Product J", "Category 2", "Description J", 15.0),
    # Category 3 - 2 products
    (7, "Product G", "Category 3", "Description G", 100.0),
    (8, "Product H", "Category 3", "Description H", 70.0),
]

# Transactions data
TRANSACTIONS_DATA = [
    # Day 1: 2025-01-01 - Total: 80.0
    (1, 1, 1, 3, "2025-01-01"),
    (2, 2, 2, 10, "2025-01-01"),
    # Day 2: 2025-01-02 - Total: 100.0
    (3, 3, 3, 5, "2025-01-02"),
    (4, 4, 4, 2, "2025-01-02"),
    (5, 5, 5, 1, "2025-01-02"),
    # Day 3: 2025-01-03 - Total: 268.0
    (6, 1, 5, 15, "2025-01-03"),
    (7, 6, 6, 1, "2025-01-03"),
    (8, 7, 3, 3, "2025-01-03"),
    (9, 4, 7, 1, "2025-01-03"),
    (10, 3, 9, 4, "2025-01-03"),
    (11, 5, 4, 1, "2025-01-03"),
    # Day 4: 2025-01-04 - Total: 125.0
    (12, 1, 8, 1, "2025-01-04"),
    (13, 2, 7, 1, "2025-01-04"),
    (14, 3, 9, 3, "2025-01-04"),
    (15, 8, 10, 1, "2025-01-04"),
    # Day 5: 2025-01-05 - Total: 190.0
    (16, 4, 4, 4, "2025-01-05"),
    (17, 6, 1, 6, "2025-01-05"),
    (18, 7, 2, 6, "2025-01-05"),
    # Day 6: 2025-01-06 - Total: 85.0
    (19, 1, 2, 5, "2025-01-06"),
    (20, 8, 4, 2, "2025-01-06"),
    (21, 5, 5, 1, "2025-01-06"),
    # Day 7: 2025-01-07 - Total: 175.0
    (22, 3, 1, 6, "2025-01-07"),
    (23, 4, 5, 1, "2025-01-07"),
    (24, 1, 3, 10, "2025-01-07"),
    (25, 7, 2, 3, "2025-01-07"),
    # Day 8: 2025-01-08 - Total: 130.0
    (26, 2, 8, 1, "2025-01-08"),
    (27, 4, 4, 2, "2025-01-08"),
    (28, 5, 5, 1, "2025-01-08"),
]
