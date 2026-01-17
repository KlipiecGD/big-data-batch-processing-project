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
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to your Google Cloud service account key file that you downloaded earlier.
- `GCP_PROJECT_ID`: Your Google Cloud project ID.

```bash
# --- Google Cloud Configuration ---
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/google-cloud-credentials.json
GCP_PROJECT_ID=your-gcp-project-id
```

### 4. Modify config/config.yaml

Update the `config/config.yaml` file to specify your GCS bucket name and BigQuery dataset name:

```yaml
cloud:
  gcs_bucket_name: "your-gcs-bucket-name"
  bq_gold_layer_dataset: "your-bigquery-dataset-name"
```

### 5. Create Consolidated `airflow.env` File

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
- For slack connection specify also slack channel in config.yaml
```yaml
dag:
  slack_channel: "#your-slack-channel"
```
*Note: The `.env` and `airflow.env` files are excluded from version control for security.*

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
2. Login using the credentials displayed in your terminal (it will be also stored manually in your folder).
3. Locate and trigger the `big_data_batch_pipeline` DAG.

### 9. Testing

To run unit tests for the project, execute the following command:

```bash
pytest tests/
```

### 10. Optimization and Experiments

To read about optimization applied, experiments regarding performance improvements and how to run them, refer to the [Optimization Experiments Documentation](experiments/documentation/experiments_report.md).