from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
import sys
from pathlib import Path

# Ensure the project root is in sys.path
project_root = str(Path(__file__).parents[3]) 
if project_root not in sys.path:
    sys.path.append(project_root)

from src.orchestration.check_db_tables_exist import check_db_tables_exist
from src.data_generation.generate_data import generate_transactions_dataset
from src.batch_processing.batch_process import run_batch_processing
from src.config.config import config

with DAG(
    dag_id='big_data_batch_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    template_searchpath=config.database.get('scripts_path', 'database_creation/'),
) as dag:

    # 1. Branching task to check if DB tables exist
    branch_task = BranchPythonOperator(
        task_id='check_db_tables_exists',
        python_callable=check_db_tables_exist
    )

    # 2. Task to create tables if they don't exist
    create_tables_task = SQLExecuteQueryOperator(
        task_id='create_postgres_tables',
        conn_id='postgres_default', 
        sql="create_tables.sql"
    )

    # 3. Task to skip setup if tables exist
    skip_setup_task = EmptyOperator(
        task_id='skip_db_setup'
    )

    # 4. Task to generate synthetic data
    generate_data_task = PythonOperator(
        task_id='generate_synthetic_data',
        python_callable=generate_transactions_dataset,
        trigger_rule='none_failed_min_one_success'
    )

    # 5. Task to run batch processing
    run_batch_processing_task = PythonOperator(
        task_id='run_batch_processing',
        python_callable=run_batch_processing,
    )

    # Define task dependencies
    branch_task >> [create_tables_task, skip_setup_task]
    [create_tables_task, skip_setup_task] >> generate_data_task 
    generate_data_task >> run_batch_processing_task