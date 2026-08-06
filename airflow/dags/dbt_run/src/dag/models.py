from pydantic import BaseModel


class DbtParams(BaseModel):
    project_dir: str = "/opt/airflow/dbt"
    profiles_dir: str = "/opt/airflow/dbt"
    run_commands: list[str] = ["run", "test"]
    model: str

class DbtDagConfig(BaseModel):
    dbt: DbtParams