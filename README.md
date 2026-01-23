# Big Data Project: Medallion Architecture with Spark Optimizations

## Introduction

This project implements a robust Big Data batch processing pipeline using the **Medallion Architecture** (Bronze, Silver, and Gold layers). It utilizes **PySpark** for scalable data transformations, **Google Cloud Storage (GCS)** for intermediate storage, and **BigQuery** for the final analytical "Gold" layer.

The pipeline is orchestrated via **Apache Airflow**, ensuring reliable execution of data generation, cleaning, and reporting tasks. A standout feature of this repository is its **Spark Optimization Experimentation Framework**, which allows for A/B testing of various Spark performance configurations (e.g., Adaptive Query Execution, Shuffle Partitioning, and Broadcast Joins) to determine the most efficient processing strategies for specific workloads.

---

## Project Structure

```text
.
├── credentials/                         # Google Cloud service account keys (not in version control)
│   └── your-service-account-key.json
├── diagrams/                            # Architecture and schema diagrams
│   ├── gold_tables.png                  # Gold layer table schemas
│   └── silver_tables.png                # Silver layer ERD
├── experiments/                         # Optimization A/B testing framework
│   ├── config/                          # Experiment-specific configurations
│   │   ├── optimization_config.yaml
│   │   └── optimization_experiment_config.py
│   ├── documentation/                   # Detailed experiment reports
│   │   ├── experiments_overview.md
│   │   └── experiments_report.md
│   ├── results/                         # Metrics and visualization charts
│   │   ├── comparison_chart.png
│   │   ├── speedup_chart.png
│   │   └── variability_plots/           # Variability plots per config
│   ├── process_gold_layer_experiment.py
│   ├── process_silver_layer_experiment.py
│   ├── run_experiments.py               # Main experiment runner
│   └── visualize_results.py             # Plot generation script
├── sql_queries/                         # Analytical SQL for Gold layer
│   ├── day_to_day_sales.sql
│   ├── performance_analysis_by_country.sql
│   ├── sales_moving_average.sql
│   ├── top_products_by_category.sql
│   └── top_spenders.sql
├── src/                                 # Main source code
│   ├── batch_processing/                # Core ETL logic
│   │   ├── clean_data.py                # Data quality and cleaning rules
│   │   ├── process_gold_layer.py        # BigQuery table creation
│   │   └── process_silver_layer.py      # Parquet generation
│   ├── cloud_utils/                     # GCS and BigQuery helpers
│   ├── config/                          # Centralized project configuration
│   ├── data_generation/                 # Synthetic data generators
│   ├── logging_utils/                   # Custom application logger
│   └── orchestration/                   # Airflow DAG definitions
│       └── dags/
│           └── dag.py
├── tests/                               # Unit and transformation tests
├── .env                                 # Environment variables (not in version control)
├── airflow.env                          # Airflow environment variables (not in version control)
├── .gitignore
├── pytest.ini
├── requirements.txt
└── README.md
```
---

## Setup

### 1. Environment Preparation

Create and activate a virtual environment, then install the required dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Google Cloud connections

Ensure you have a Google Cloud project set up with the necessary services (e.g., BigQuery, Cloud Storage). Create a service account with the required permissions and download the JSON key file. Put the key file in a secure location excluded from version control.

Also authenticate your gcloud CLI:

```bash
gcloud auth login
```

### 3. Create Consolidated `.env` File

Create a `.env` file in the project root. Add path to your Google Cloud service account key file and other necessary configurations. Replace placeholders with your actual values:

* `GOOGLE_APPLICATION_CREDENTIALS`: Path to your Google Cloud service account key file that you downloaded earlier.

```bash
# --- Google Cloud Configuration ---
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/google-cloud-credentials.json
```

### 4. Create Consolidated `airflow.env` File

```bash
# --- Airflow Configuration ---
# Sets Airflow home and DAGs folder to your current project directory
export AIRFLOW_HOME=$(pwd)

# Points Airflow to your local DAGs directory
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/src/orchestration/dags

# If you want to use callbacks for success/failure notifications on Slack, set up Slack connection
export AIRFLOW_CONN_SLACK_CONN='{
    "conn_type": "slackapi",
    "password": "your-slack-bot-token-starting-with-xoxb"
}'
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```
### 5. Modify `config/config.yaml`

Update the `src/config/config.yaml` file to specify all necessary parameters such as GCS bucket names, BigQuery dataset names, and other project-specific settings.

### 6. Load Environment and Initialize Airflow

Every time you open a new terminal for this project, you must load the configuration. Then, initialize the Airflow metadata database:

```bash
source airflow.env
airflow db migrate
```

### 7. Start Airflow

Run Airflow in standalone mode. This will start all necessary components (webserver, scheduler, etc.):

```bash
airflow standalone
```

### 8. Access the Pipeline

1. Open your web browser and go to `http://localhost:8080`.
2. Login using the credentials displayed in your terminal.
3. Locate and trigger the `big_data_batch_pipeline` DAG.

### 9. Testing

To run unit tests for the project, execute the following command:

```bash
pytest tests/
```

### 10. Optimization and Experiments

To read about optimization applied, experiments regarding performance improvements and how to run them, refer to the [Optimization Experiments Documentation](experiments/documentation/experiments_overview.md) and [Experiments Report](experiments/documentation/experiments_report.md).