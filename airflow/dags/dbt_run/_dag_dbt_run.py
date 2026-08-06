from pathlib import Path

from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

from common_config.dag_factory import create_dynamic_dags

from dbt_run.src.dag.models import DbtDagConfig

DAG_PATH = Path(__file__).resolve().parent

for dag, asset_name, dag_config, airflow_config in create_dynamic_dags(DAG_PATH, config_model=DbtDagConfig):

    # Register the DAG in the global namespace
    globals()[dag.dag_id] = dag

    # get the dbt parameters from the dag_config
    dbt_params = dag_config.dbt

    with dag:

        bash_command_template = (
            "dbt {dbt_run_command} "
            f"--project-dir {dbt_params.project_dir} "
            f"--profiles-dir {dbt_params.profiles_dir} "
            f"--select {dbt_params.model} "
            '--vars \'{{"partition_path": "{{{{ data_interval_end.strftime("year=%Y/month=%m/day=%d") }}}}"}}\''
        )

        # Run dbt commands for each command specified in the config
        for dbt_run_command in dbt_params.run_commands:
            task_id = f"{dbt_run_command}_dbt_model"
            BashOperator(
                task_id=task_id,
                bash_command=bash_command_template.format(dbt_run_command=dbt_run_command),
                outlets=[Dataset(f"{dag.dag_id}.{task_id}")],
            )
