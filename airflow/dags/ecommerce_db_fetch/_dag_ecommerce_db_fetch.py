from pathlib import Path

from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

from utils.dag_factory import create_dag

from ecommerce_db_fetch.src.dag.models import FetchDagConfig
from ecommerce_db_fetch.src.dag.functions import extract_table_to_minio_parquet

# Get the DAG Path (directory of this file)
DAG_PATH = Path(__file__).resolve().parent

# Returns DAG, validated Pydantic dag_config, and validated AirflowConfigModel
dag, dag_config, _ = create_dag(DAG_PATH, config_model=FetchDagConfig)

with dag:
    # Dynamically create ingestion tasks from config.yaml table list
    for table_config in dag_config.ecommerce.tables:

        table_name = table_config.name
        task_id = f"ingest_{table_name}_to_bronze"

        PythonOperator(
            task_id=task_id,        
            python_callable=extract_table_to_minio_parquet,
            op_kwargs=dict(
                table_name=table_name,
            ),
            outlets=[Dataset(f"{dag.dag_id}.{task_id}")],  # Define the dataset for downstream dependencies
        )
    