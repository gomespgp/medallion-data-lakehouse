import yaml
from pathlib import Path
from datetime import datetime, timedelta

from ecommerce_db_fetch.functions import extract_table_to_minio_parquet

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Load Configuration dynamically relative to this file's directory
# ---------------------------------------------------------------------------
DAG_DIR = Path(__file__).resolve().parent
CONFIG_PATH = DAG_DIR / "config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_yaml(f) if hasattr(yaml, "safe_yaml") else yaml.safe_load(f)

AIRFLOW_CONFIG = config.get("airflow", {})
ECOMMERCE_CONFIG = config.get("ecommerce", {})

# ---------------------------------------------------------------------------
# Default Arguments & Environment Configs
# ---------------------------------------------------------------------------
owners_list = AIRFLOW_CONFIG.get("owners", {}).get("technical_owners", ["data_engineering"])
owner_str = ", ".join(owners_list) if isinstance(owners_list, list) else str(owners_list)

default_args = {
    'owner': owner_str,
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}



# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id=AIRFLOW_CONFIG.get("id"),
    default_args=default_args,
    schedule=AIRFLOW_CONFIG.get("schedule"),
    catchup=False,
    tags=['ingestion', 'bronze', 'postgres', 'minio'],
) as dag:

    # Dynamically create ingestion tasks from config.yaml table list
    for table_config in ECOMMERCE_CONFIG.get("tables", []):
        table_name = table_config.get("name")
        task_id = f"ingest_{table_name}_to_bronze"
        PythonOperator(
            task_id=task_id,        
            python_callable=extract_table_to_minio_parquet,
            op_kwargs={'table_name': table_name},
            outlets=[Dataset(f"{dag.dag_id}.{task_id}")],  # Define the dataset for downstream dependencies
        )
 