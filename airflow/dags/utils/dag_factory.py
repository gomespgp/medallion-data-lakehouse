from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar, Union
from datetime import datetime, timedelta

import yaml
from pydantic import BaseModel, Field
from airflow import DAG
from airflow.datasets import Dataset

T = TypeVar("T", bound=BaseModel)


# ---------------------------------------------------------------------------
# Pydantic Schemas for Airflow Settings
# ---------------------------------------------------------------------------
class TaskDependency(BaseModel):
    dag_id: str
    task_id: str


class AirflowOwners(BaseModel):
    technical_owners: Union[List[str], str] = Field(default_factory=lambda: ["data_engineering"])


class AirflowConfigModel(BaseModel):
    id: Optional[str] = None
    schedule: Optional[str] = None
    dependencies: List[TaskDependency] = Field(default_factory=list)
    owners: AirflowOwners = Field(default_factory=AirflowOwners)
    retries: int = 1
    retry_delay_minutes: int = 1
    start_date: str = "2026-01-01"
    catchup: bool = False
    tags: List[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Core Configuration Resolvers & Helpers
# ---------------------------------------------------------------------------
def _deep_merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merges override dict into base dict."""
    merged = base.copy()
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


# ---------------------------------------------------------------------------
# Core Builder
# ---------------------------------------------------------------------------
def _build_dag_instance(
    resolved_dag_id: str, airflow_meta: AirflowConfigModel
) -> Tuple[DAG, AirflowConfigModel]:
    """Instantiates an Airflow DAG from a validated AirflowConfigModel."""
    
    # Resolve owner string properly
    owners = airflow_meta.owners.technical_owners
    if isinstance(owners, list):
        owner_str = ", ".join(owners)
    else:
        owner_str = str(owners)

    # Schedule: Datasets take priority if declared; fallback to cron string or None
    schedule_arg: Union[str, List[Dataset], None] = None
    if airflow_meta.dependencies:
        schedule_arg = [
            Dataset(f"{dep.dag_id}.{dep.task_id}") for dep in airflow_meta.dependencies
        ]
    elif airflow_meta.schedule:
        schedule_arg = airflow_meta.schedule

    default_args = {
        "owner": owner_str,
        "depends_on_past": False,
        "start_date": datetime.strptime(airflow_meta.start_date, "%Y-%m-%d"),
        "email_on_failure": False,
        "retries": airflow_meta.retries,
        "retry_delay": timedelta(minutes=airflow_meta.retry_delay_minutes),
    }

    dag = DAG(
        dag_id=resolved_dag_id,
        default_args=default_args,
        schedule=schedule_arg,
        catchup=airflow_meta.catchup,
        tags=airflow_meta.tags or [resolved_dag_id],
    )

    return dag, airflow_meta


# ---------------------------------------------------------------------------
# Public Factory Functions
# ---------------------------------------------------------------------------
def create_dag(
    dag_path: Path,
    config_model: Optional[Type[T]] = None,
) -> Tuple[DAG, Union[T, Dict[str, Any]], AirflowConfigModel]:
    """
    Creates a single DAG using `DAG_PATH/src/dag/config.yaml`.
    
    Returns:
        (dag, dag_config, airflow_config)
    """
    config_file = dag_path / "src" / "dag" / "config.yaml"
    if not config_file.exists():
        raise FileNotFoundError(f"Global configuration not found at: {config_file}")

    with open(config_file, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f) or {}

    if "airflow" not in raw_config:
        raise KeyError(f"Missing required 'airflow' key in {config_file}")

    airflow_meta = AirflowConfigModel(**raw_config.get("airflow", {}))
    resolved_dag_id = airflow_meta.id or dag_path.name

    dag, resolved_airflow_config = _build_dag_instance(resolved_dag_id, airflow_meta)

    # Extract non-airflow config key(s)
    raw_dag_config = {k: v for k, v in raw_config.items() if k != "airflow"}
    
    # Parse with Pydantic model if provided, else raw dict
    if config_model:
        dag_config = config_model(**raw_dag_config)
    else:
        dag_config = raw_dag_config

    return dag, dag_config, resolved_airflow_config


def create_dynamic_dags(
    dag_path: Path,
    config_model: Optional[Type[T]] = None,
) -> List[Tuple[DAG, str, Union[T, Dict[str, Any]], AirflowConfigModel]]:
    """
    Loads base config from `DAG_PATH/src/dag/config.yaml`, merges it with each YAML 
    file in `DAG_PATH/configs/*.yaml`, and generates dynamic DAGs.

    Returns:
        List of tuples: (dag, asset_name, dag_config, airflow_config)
    """
    base_config_file = dag_path / "src" / "dag" / "config.yaml"
    configs_dir = dag_path / "configs"

    if not base_config_file.exists():
        raise FileNotFoundError(f"Global base config not found at: {base_config_file}")

    if not configs_dir.exists():
        raise FileNotFoundError(f"Configs directory not found at: {configs_dir}")

    with open(base_config_file, "r", encoding="utf-8") as f:
        base_raw_config = yaml.safe_load(f) or {}

    dynamic_dags = []

    for config_file in configs_dir.glob("*.yaml"):
        asset_name = config_file.stem  # e.g., 'gold' or 'silver'

        with open(config_file, "r", encoding="utf-8") as f:
            asset_raw_config = yaml.safe_load(f) or {}

        # Merge global base config with asset-specific config (asset overrides global)
        merged_config = _deep_merge_dicts(base_raw_config, asset_raw_config)

        if "airflow" not in merged_config:
            raise KeyError(f"Missing required 'airflow' key after merge for {config_file.name}")

        airflow_meta = AirflowConfigModel(**merged_config.get("airflow", {}))
        
        # Priority: asset file explicit id > global base id > default pattern (e.g. dbt_run_gold)
        resolved_dag_id = asset_raw_config.get("airflow", {}).get("id") or airflow_meta.id or f"{dag_path.name}_{asset_name}"

        dag, resolved_airflow_config = _build_dag_instance(resolved_dag_id, airflow_meta)

        # Extract non-airflow configs
        raw_dag_config = {k: v for k, v in merged_config.items() if k != "airflow"}

        if config_model:
            dag_config = config_model(**raw_dag_config)
        else:
            dag_config = raw_dag_config

        dynamic_dags.append((dag, asset_name, dag_config, resolved_airflow_config))

    return dynamic_dags