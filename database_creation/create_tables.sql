-- create_tables.sql
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    country VARCHAR(100),
    address TEXT,
    signup_date DATE
);

CREATE TABLE IF NOT EXISTS products (
    product_id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    description TEXT,
    price DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BIGINT PRIMARY KEY,
    user_id BIGINT REFERENCES users(user_id),
    product_id BIGINT REFERENCES products(product_id),
    quantity INT,
    transaction_date DATE
);