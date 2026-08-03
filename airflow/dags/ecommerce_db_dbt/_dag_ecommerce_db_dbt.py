import yaml
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

# Load Configuration
DAG_DIR = Path(__file__).resolve().parent
CONFIG_PATH = DAG_DIR / "config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

AIRFLOW_CONFIG = config.get("airflow", {})
DBT_CONFIG = config.get("dbt", {})

owners_list = AIRFLOW_CONFIG.get("owners", {}).get("technical_owners", ["data_engineering"])
owner_str = ", ".join(owners_list) if isinstance(owners_list, list) else str(owners_list)

default_args = {
    'owner': owner_str,
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

PROJECT_DIR = DBT_CONFIG.get("project_dir", "/opt/airflow/dbt")
PROFILES_DIR = DBT_CONFIG.get("profiles_dir", "/opt/airflow/dbt")

# Get dependencies from config.yaml (Datasets)
dependencies = [Dataset(f"{dep['dag_id']}.{dep['task_id']}") for dep in AIRFLOW_CONFIG.get("dependencies", [])]

with DAG(
    dag_id=AIRFLOW_CONFIG.get("id", "ecommerce_transformation"),
    default_args=default_args,
    schedule=dependencies,
    catchup=False,
    tags=['dbt', 'silver', 'gold', 'duckdb', 'transformation'],
) as dag:

    # 1. Run Silver Models (Staging & Deduplication)
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"dbt run --project-dir {PROJECT_DIR} --profiles-dir {PROFILES_DIR} --select silver",
    )

    # 2. Run Gold Models (Star Schema & Business Aggregations)
    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"dbt run --project-dir {PROJECT_DIR} --profiles-dir {PROFILES_DIR} --select gold",
    )

    # 3. Run dbt Data Quality Tests
    dbt_test = BashOperator(
        task_id="dbt_test_all",
        bash_command=f"dbt test --project-dir {PROJECT_DIR} --profiles-dir {PROFILES_DIR}",
    )

    # Pipeline Flow
    dbt_run_silver >> dbt_run_gold >> dbt_test