## Setup

### 1. Environment Preparation

Create and activate a virtual environment, then install the required dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

```

### 2. Configure PostgreSQL Database

Ensure PostgreSQL is installed and running. Create a dedicated database for the project:

```sql
CREATE DATABASE bigdata_db;
```

### 3. Create Consolidated `.env` File

Create a `.env` file in the project root. This file will handle your database credentials, Airflow paths, and the database connection automatically.

**Note:** Ensure you replace `your_username` and `your_password` with your actual local PostgreSQL credentials.

```bash
# --- Database Configuration ---
export DB_NAME=bigdata_db
export DB_USER=your_username
export DB_PASSWORD=your_password
export DB_HOST=localhost
export DB_PORT=5432

# --- Airflow Configuration ---
# Sets Airflow home and DAGs folder to your current project directory
export AIRFLOW_HOME=$(pwd)

# Points Airflow to your local DAGs directory
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/src/orchestration/dags

# --- Airflow Connection (Automatic) ---
# This environment variable automatically creates the 'postgres_default' connection 
# used in the DAGs. 
export AIRFLOW_CONN_POSTGRES_DEFAULT='{
    "conn_type": "postgres",
    "login": "your_username",
    "password": "your_password",
    "host": "localhost",
    "port": 5432,
    "schema": "bigdata_db"
}'

```

*Note: The `.env` file is excluded from version control for security.*

### 4. Load Environment and Initialize Airflow

Every time you open a new terminal for this project, you must load the configuration. Then, initialize the Airflow metadata database:

```bash
source .env
airflow db migrate
```

### 5. Start Airflow

Run Airflow in standalone mode. This will start all necessary components (webserver, scheduler, etc.):

```bash
airflow standalone
```

### 6. Access the Pipeline

1. Open your web browser and go to `http://localhost:8080`.
2. Login using the credentials displayed in your terminal (it will be also stored manually in your folder).
3. You will not see the connection in the **Admin -> Connections** UI because it is loaded dynamically from your `.env`.
4. Locate and trigger the `big_data_batch_pipeline` DAG.