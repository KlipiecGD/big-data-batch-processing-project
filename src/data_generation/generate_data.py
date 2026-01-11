import pandas as pd
from faker import Faker
import random
import os

from src.config.config import config
from src.logging_utils.logger import logger

fake = Faker()
# If we want to ensure reproducibility
# fake.seed_instance(2137)

data_path = config.data_generation.get("data_path", "data/")
if not os.path.exists(data_path):
    os.makedirs(data_path)

def generate_transactions_dataset(users_count: int = config.data_generation.get("users_count", 2000), products_count: int = config.data_generation.get("items_count", 1000), transactions_count: int = config.data_generation.get("transactions_count", 12000)) -> None:
    """Generate a synthetic transactions dataset and save it as a CSV files.

    Args:
        users_count (int): Number of unique users
        products_count (int): Number of unique products
        transactions_count (int): Number of transactions to generate
    """
    logger.info("Generating synthetic transactions dataset...")
    # Generate users
    logger.info("Generating users...")
    users = []
    for i in range(1, users_count + 1):
        users.append({
            "user_id": i,
            "name": fake.name(),
            "email": fake.email(),
            "country": fake.country(),
            "address": fake.address().replace("\n", " "),
            "signup_date": fake.date_between(start_date='-2y', end_date='-30d')
        })
    # Save users to CSV
    df_users = pd.DataFrame(users)
    df_users.to_csv(os.path.join(data_path, "users.csv"), index=False)
    logger.info(f"Generated {len(users)} users. Saved to {data_path}users.csv")

    # Generate products
    logger.info("Generating products...")
    products = []
    categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Toys', 'Sports', 'Beauty', 'Automotive']
    for i in range(1, products_count + 1):
        products.append({
            "product_id": i,
            "name": fake.word().capitalize() + " " + fake.word(),
            "category": random.choice(categories),
            "description": fake.sentence(),
            "price": round(random.uniform(10.0, 500.0), 2)
        })
    # Save products to CSV
    df_products = pd.DataFrame(products)
    df_products.to_csv(os.path.join(data_path, "products.csv"), index=False)
    logger.info(f"Generated {len(products)} products. Saved to {data_path}products.csv")

    # Generate transactions
    logger.info("Generating transactions...")
    transactions = []

    # Convert users DataFrame to list of dicts for easy access
    users_list = df_users.to_dict(orient="records")

    for i in range(1, transactions_count + 1):
        selected_user = random.choice(users_list)
        u_id = selected_user['user_id']
        u_signup = selected_user['signup_date'] # Ensure transaction date is after signup date
        transactions.append({
            "transaction_id": i,
            "user_id": u_id,
            "product_id": random.randint(1, products_count),
            "quantity": random.randint(1, 5),
            "transaction_date": fake.date_between(start_date=u_signup, end_date='today')
        })
    # Save transactions to CSV
    df_transactions = pd.DataFrame(transactions)
    df_transactions.to_csv(os.path.join(data_path, "transactions.csv"), index=False)
    logger.info(f"Generated {len(transactions)} transactions. Saved to {data_path}transactions.csv")

if __name__ == "__main__":
    generate_transactions_dataset()
    logger.info("Data generation completed.")
