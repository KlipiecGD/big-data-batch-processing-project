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
        StructField("user_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("address", StringType(), True),
        StructField("signup_date", StringType(), True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), True),
    ]
)

TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("transaction_date", StringType(), True),
    ]
)

# Test data for users with null IDs
USERS_NULL_IDS = [
    (None, "John Doe", "john@example.com", "USA", "123 Main St", "2023-01-15"), # Invalid: null ID
    (2, "Jane Smith", "jane@example.com", "UK", "456 Oak Ave", "2023-02-20"), # Valid
    (3, "Bob Johnson", "bob@example.com", "Canada", "789 Pine Rd", "2023-03-10"), # Valid
]

# Test data for users with invalid IDs (≤ 0)
USERS_INVALID_IDS = [
    (0, "Alice Brown", "alice@example.com", "USA", "111 Elm St", "2023-04-05"), # Invalid: zero ID
    (-1, "Charlie Davis", "charlie@example.com", "UK", "222 Maple Dr", "2023-05-12"), # Invalid: negative ID
    (-99, "Diana Evans", "diana@example.com", "Canada", "333 Birch Ln", "2023-06-18"), # Invalid: negative ID
    (1, "Frank Green", "frank@example.com", "USA", "444 Cedar Ct", "2023-07-22"), # Valid
    (2, "Grace Hill", "grace@example.com", "UK", "555 Spruce Way", "2023-08-30"), # Valid
]

# Test data for users with invalid emails
USERS_INVALID_EMAILS = [
    (1, "User One", "invalid_email", "USA", "Address 1", "2023-01-01"), # Invalid: no @
    (2, "User Two", "", "UK", "Address 2", "2023-02-01"), # Invalid: empty email
    (3, "User Three", None, "Canada", "Address 3", "2023-03-01"), # Invalid: null email
    (4, "User Four", "user_at_example.com", "USA", "Address 4", "2023-04-01"), # Invalid: malformed email
    (5, "User Five", "valid@example.com", "UK", "Address 5", "2023-05-01"), # Valid
    (6, "User Six", "another@valid.com", "Canada", "Address 6", "2023-06-01"), # Valid
]

# Test data for users with invalid dates
USERS_INVALID_DATES = [
    (1, "User A", "usera@example.com", "USA", "Address A", "1899-01-01"), # Invalid: too old
    (2, "User B", "userb@example.com", "UK", "Address B", "1500-12-31"), # Invalid: too old
    (3, "User C", "userc@example.com", "Canada", "Address C", "invalid-date"), # Invalid: malformed
    (4, "User D", "userd@example.com", "USA", "Address D", None), # Invalid: null
    (5, "User E", "usere@example.com", "UK", "Address E", "2023-01-15"), # Valid
    (6, "User F", "userf@example.com", "Canada", "Address F", "2023-06-20"), # Valid
]

# Test data for valid users
USERS_VALID = [
    (1, "John Doe", "john@example.com", "USA", "123 Main St", "2023-01-15"), # Valid
    (2, "Jane Smith", "jane@example.com", "UK", "456 Oak Ave", "2023-02-20"), # Valid
]

# Test data for users (referential integrity testing)
USERS_REFERENTIAL = [
    (1, "User One", "user1@example.com", "USA", "Address 1", "2023-01-01"), # Valid
    (2, "User Two", "user2@example.com", "UK", "Address 2", "2023-02-01"), # Valid
    (3, "User Three", "user3@example.com", "Canada", "Address 3", "2023-03-01"), # Valid
]

# Test data for products with invalid prices
PRODUCTS_INVALID_PRICES = [
    (1, "Product A", "Category 1", "Description A", -10.0), # Invalid: negative price
    (2, "Product B", "Category 2", "Description B", 0.0), # Invalid: zero price
    (3, "Product C", "Category 3", "Description C", -99.0), # Invalid: negative price
    (4, "Product D", "Category 1", "Description D", None), # Invalid: null price
    (5, "Product E", "Category 2", "Description E", 50.0), # Valid
    (6, "Product F", "Category 3", "Description F", 100.0), # Valid
]

# Test data for products with empty fields
PRODUCTS_EMPTY_FIELDS = [
    (1, "", "Category 1", "Description 1", 10.0), # Invalid: empty name
    (2, "Product 2", "", "Description 2", 20.0), # Invalid: empty category
    (3, "Product 3", "Category 3", "", 30.0), # Invalid: empty description
    (4, None, "Category 4", "Description 4", 40.0), # Invalid: null name
    (5, "Product 5", None, "Description 5", 50.0), # Invalid: null category
    (6, "Product 6", "Category 6", None, 60.0), # Invalid: null description
    (7, "Product 7", "Category 7", "Description 7", 70.0), # Valid
    (8, "Product 8", "Category 8", "Description 8", 80.0), # Valid
]

# Test data for valid products
PRODUCTS_VALID = [
    (1, "Laptop", "Electronics", "High-performance laptop", 999.99), # Valid
    (2, "Mouse", "Electronics", "Wireless mouse", 29.99), # Valid
]

# Test data for products (referential integrity testing)
PRODUCTS_REFERENTIAL = [
    (10, "Product Ten", "Category A", "Description 10", 100.0), # Valid
    (20, "Product Twenty", "Category B", "Description 20", 200.0), # Valid
    (30, "Product Thirty", "Category C", "Description 30", 300.0), # Valid
]

# Test data for transactions with invalid quantities
TRANSACTIONS_INVALID_QUANTS = [
    (1, 1, 10, 0, "2025-01-01"), # Invalid: zero quantity
    (2, 1, 20, -1, "2025-01-02"), # Invalid: negative quantity
    (3, 2, 30, -99, "2025-01-03"), # Invalid: negative quantity
    (4, 2, 10, None, "2025-01-04"), # Invalid: null quantity
    (5, 3, 20, 5, "2025-01-05"), # Valid
    (6, 3, 30, 3, "2025-01-06"), # Valid
]

# Test data for transactions with invalid dates
TRANSACTIONS_INVALID_DATES = [
    (1, 1, 10, 2, "1899-01-01"), # Invalid: too old
    (2, 1, 20, 3, "1500-12-31"), # Invalid: too old
    (3, 2, 30, 1, "invalid-date"), # Invalid: not a date
    (4, 2, 10, 4, None), # Invalid: null
    (5, 3, 20, 2, "2025-01-15"), # Valid
    (6, 3, 30, 5, "2025-01-20"), # Valid
]

# Test data for transactions (referential integrity testing)
TRANSACTIONS_REFERENTIAL = [
    (1, 1, 10, 2, "2025-01-01"),  # Valid: user_id=1, product_id=10
    (2, 2, 20, 3, "2025-01-02"),  # Valid: user_id=2, product_id=20
    (3, 3, 30, 1, "2025-01-03"),  # Valid: user_id=3, product_id=30
    (4, 99, 10, 4, "2025-01-04"),  # Invalid: user_id=99 doesn't exist
    (5, 1, 999, 2, "2025-01-05"),  # Invalid: product_id=999 doesn't exist
    (6, 888, 777, 3, "2025-01-06"),  # Invalid: both don't exist
]
