

## Setup

1. Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Create a .env file in the project root with the following content:
```env
# Database Configuration
DB_NAME=bigdata_db
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

3. Set AIRFLOW_HOME environment variable:
```bash
export AIRFLOW_HOME=/Path/To/Your/BigDataProject
```

4. Initialize the Airflow database:
```bash
airflow db migrate
```
5. Set the dags folder environment variable:

```bash
export AIRFLOW__CORE__DAGS_FOLDER=/Path/To/Your/BigDataProject/src/orchestration/dags
```
6. Start Airflow in standalone mode:
```bash
airflow standalone
```

7. Add PostgreSQL connection to Airflow:
```bash
airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-login 'your_username' \
    --conn-schema 'your_schema' \
    --conn-port 5432
```
Or add it manually in the Airflow UI.

