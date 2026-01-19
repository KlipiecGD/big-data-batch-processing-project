from airflow import DAG
from airflow.providers.slack.notifications.slack import SlackNotifier
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
from src.logging_utils.logger import logger


def success_callback(context: Context) -> None:
    """
    Callback function to be called on DAG success.
    Args:
        context (Context): Airflow context object containing DAG run information.
    """
    dag_run = context.get("dag_run")
    if dag_run:
        dag_id = dag_run.dag_id
        logger.info(f"DAG {dag_id} completed successfully!")
        logger.info((f"Start Time: {dag_run.start_date}, End Time: {dag_run.end_date}"))



def failure_callback(context: Context) -> None:
    """
    Callback function to be called on DAG failure.
    Args:
        context (Context): Airflow context object containing DAG run information.
    """
    dag_run = context.get("dag_run")
    if dag_run:
        dag_id = dag_run.dag_id
        logger.error(f"DAG {dag_id} failed!")
        logger.error((f"Start Time: {dag_run.start_date}, End Time: {dag_run.end_date}"))
        logger.error(f"Error Message: {context.get('exception')}")

slack_channel = config.dag.get("slack_channel", "#all-airflow")

slack_failure_notifier = SlackNotifier(
    slack_conn_id="slack_conn", 
    text=(
        ":red_circle: *DAG Failure Alert*\n"
        "*DAG:* {{ dag.dag_id }}\n"
        "*Task:* {{ ti.task_id }}\n"
        # "*Error:* `{{ exception }}`\n" - it can be too long
        "<{{ ti.log_url }}|View Logs>"
    ),
    channel=slack_channel
)

slack_success_notifier = SlackNotifier(
    slack_conn_id="slack_conn", 
    text=":large_green_circle: DAG *{{ dag.dag_id }}* completed successfully!",
    channel=slack_channel
)

with DAG(
    dag_id="big_data_batch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    description="Bigdata Medallion Architecture Batch Processing Pipeline",
    tags=["bigdata", "batch", "medallion_architecture"],
    on_success_callback=[success_callback, slack_success_notifier],
    on_failure_callback=[failure_callback, slack_failure_notifier],
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
