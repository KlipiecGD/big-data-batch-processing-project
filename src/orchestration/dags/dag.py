from airflow import DAG
from airflow.sdk import Context
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime
import sys
from pathlib import Path

# Ensure the project root is in sys.path
project_root = str(Path(__file__).parents[3])
if project_root not in sys.path:
    sys.path.append(project_root)

from src.data_generation.generate_bronze_layer_data import generate_transactions_dataset
from src.batch_processing.process_silver_layer import run_silver_layer_transformations
from src.batch_processing.process_gold_layer import run_gold_layer_creation
from src.config.config import config


def success_callback(context: Context) -> None:
    """Callback function to be called on DAG success."""


def failure_callback(context: Context) -> None:
    """Callback function to be called on DAG failure."""


with DAG(
    dag_id="big_data_batch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    description="Bigdata Medallion Architecture Batch Processing Pipeline",
    tags=["bigdata", "batch", "medallion_architecture"],
    on_success_callback=success_callback,
    on_failure_callback=failure_callback,
) as dag:
    # 1. Generate Bronze Layer Data
    generate_bronze_data = PythonOperator(
        task_id="generate_bronze_data",
        python_callable=generate_transactions_dataset,
        retries=config.dag.get("retries", 2),
        retry_delay=config.dag.get("retries_delay", 60),
    )

    # 2. Process Silver Layer
    process_silver_layer = PythonOperator(
        task_id="process_silver_layer",
        python_callable=run_silver_layer_transformations,
        retries=config.dag.get("retries", 2),
        retry_delay=config.dag.get("retries_delay", 60),
    )

    # 3. Process Gold Layer
    process_gold_layer = PythonOperator(
        task_id="process_gold_layer",
        python_callable=run_gold_layer_creation,
        retries=config.dag.get("retries", 2),
        retry_delay=config.dag.get("retries_delay", 60),
    )

    # Define task dependencies
    generate_bronze_data >> process_silver_layer >> process_gold_layer
