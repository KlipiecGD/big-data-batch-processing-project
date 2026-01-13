import random
import os
import numpy as np
import pandas as pd
from faker import Faker

from src.config.config import config
from src.logging_utils.logger import logger

fake = Faker()
# If we want to ensure reproducibility
# fake.seed_instance(2137)

data_path = config.data_generation.get("data_path", "bronze_layer/")
if not os.path.exists(data_path):
    os.makedirs(data_path)


def inject_noise(
    df: pd.DataFrame,
    noise_level: float = config.data_generation.get("noise_level", 0.03),
    null_wrong_proportion: float = config.data_generation.get(
        "null_wrong_proportion", 0.5
    ),
) -> pd.DataFrame:
    """
    Inject noise into the DataFrame by randomly corrupting a percentage of its entries.
    Args:
        df (pd.DataFrame): The original DataFrame.
        noise_level (float): The proportion of entries to corrupt (between 0 and 1).
        null_wrong_proportion (float): The proportion of corrupted entries to set to null vs. wrong values.
        For example, a value of 0.7 means 70% nulls and 30% wrong values.
    Returns:
        pd.DataFrame: The DataFrame with injected noise.
    """
    df_noisy = df.copy()
    n_rows = len(df_noisy)
    n_corrupt = int(n_rows * noise_level)

    indices_to_corrupt = random.sample(range(n_rows), n_corrupt)

    for idx in indices_to_corrupt:
        col = random.choice(df_noisy.columns)

        #
        if random.random() <= null_wrong_proportion:
            df_noisy.at[idx, col] = np.nan
        else:
            # Logic for "Wrong" values based on column type/name
            if "price" in col or "quantity" in col:
                df_noisy.at[idx, col] = random.choice([-1, -99, 0])  # Invalid numbers
            elif "email" in col:
                val = str(df_noisy.at[idx, col])
                df_noisy.at[idx, col] = val.replace("@", "_at_")  # Invalid email format
            elif "date" in col:
                df_noisy.at[idx, col] = fake.date_between(
                    start_date="-500y", end_date="-100y"
                )  # Logical outlier
            else:
                df_noisy.at[idx, col] = ""  # Empty string

    return df_noisy


def generate_transactions_dataset(
    users_count: int = config.data_generation.get("users_count", 10000),
    products_count: int = config.data_generation.get("items_count", 10000),
    transactions_count: int = config.data_generation.get("transactions_count", 18000),
    noise_level: float = config.data_generation.get("noise_level", 0.03),
    null_wrong_proportion: float = config.data_generation.get(
        "null_wrong_proportion", 0.5
    ),
) -> None:
    """Generate a synthetic transactions dataset and save it as a CSV files.

    Args:
        users_count (int): Number of unique users
        products_count (int): Number of unique products
        transactions_count (int): Number of transactions to generate
        noise_level (float): Proportion of entries to corrupt with noise
        null_wrong_proportion (float): Proportion of corrupted entries to set to null vs. wrong values
    """
    logger.info("Generating synthetic transactions dataset...")
    # Generate users
    logger.info("Generating users...")
    users = []
    for i in range(1, users_count + 1):
        users.append(
            {
                "user_id": i,
                "name": fake.name(),
                "email": fake.email(),
                "country": fake.country(),
                "address": fake.address().replace("\n", " "),
                "signup_date": fake.date_between(start_date="-2y", end_date="-30d"),
            }
        )
    # Inject noise and save users to CSV
    df_users = inject_noise(pd.DataFrame(users), noise_level, null_wrong_proportion)
    df_users.to_csv(os.path.join(data_path, "users.csv"), index=False)
    logger.info(f"Generated {len(users)} users. Saved to {data_path}users.csv")

    # Generate products
    logger.info("Generating products...")
    products = []
    categories = [
        "Electronics",
        "Books",
        "Clothing",
        "Home",
        "Toys",
        "Sports",
        "Beauty",
        "Automotive",
    ]
    for i in range(1, products_count + 1):
        products.append(
            {
                "product_id": i,
                "name": fake.word().capitalize() + " " + fake.word(),
                "category": random.choice(categories),
                "description": fake.sentence(),
                "price": round(random.uniform(10.0, 500.0), 2),
            }
        )
    # Inject noise and save products to CSV
    df_products = inject_noise(pd.DataFrame(products), noise_level, null_wrong_proportion)
    df_products.to_csv(os.path.join(data_path, "products.csv"), index=False)
    logger.info(f"Generated {len(products)} products. Saved to {data_path}products.csv")

    # Generate transactions
    logger.info("Generating transactions...")
    transactions = []

    # Convert users DataFrame to list of dicts for easy access
    users_list = df_users.to_dict(orient="records")

    for i in range(1, transactions_count + 1):
        selected_user = random.choice(users_list)
        u_id = selected_user["user_id"]
        u_signup = selected_user[
            "signup_date"
        ]  # Ensure transaction date is after signup date
        transactions.append(
            {
                "transaction_id": i,
                "user_id": u_id,
                "product_id": random.randint(1, products_count),
                "quantity": random.randint(1, 5),
                "transaction_date": fake.date_between(
                    start_date=u_signup, end_date="today"
                ),
            }
        )
    # Inject noise and save transactions to CSV
    df_transactions = inject_noise(pd.DataFrame(transactions), noise_level, null_wrong_proportion)
    df_transactions.to_csv(os.path.join(data_path, "transactions.csv"), index=False)
    logger.info(
        f"Generated {len(transactions)} transactions. Saved to {data_path}transactions.csv"
    )


if __name__ == "__main__":
    generate_transactions_dataset()
    logger.info("Data generation completed.")
