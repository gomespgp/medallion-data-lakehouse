from pathlib import Path

from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

from utils.dag_factory import create_dynamic_dags

from dbt_run.src.dag.models import DbtDagConfig

DAG_PATH = Path(__file__).resolve().parent

for dag, asset_name, dag_config, airflow_config in create_dynamic_dags(DAG_PATH, config_model=DbtDagConfig):

    # Register the DAG in the global namespace
    globals()[dag.dag_id] = dag

    # get the dbt parameters from the dag_config
    dbt_params = dag_config.dbt

    with dag:
        # 1. Define task
        task_id = "run_dbt_model"
        dbt_run_model_task = BashOperator(
            task_id=task_id,
            bash_command=f"dbt run --project-dir {dbt_params.project_dir} --profiles-dir {dbt_params.profiles_dir} --select {dbt_params.model}",
            outlets=[Dataset(f"{dag.dag_id}.{task_id}")],  # Define the dataset for downstream dependencies
        )
        
        # 2. Define test task
        dbt_test_model_task = BashOperator(
            task_id="test_dbt_model",
            bash_command=f"dbt test --project-dir {dbt_params.project_dir} --profiles-dir {dbt_params.profiles_dir} --select {dbt_params.model}",
    )
