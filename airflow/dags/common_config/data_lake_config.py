import os
from typing import Dict, Literal

# 1. Single source of truth for execution environment
ENVIRONMENT = os.getenv("ENV", "dev").strip().lower()

LAYER = Literal["bronze", "silver", "gold", "logs"]


def get_s3_bucket(layer: LAYER) -> str:
    """
    Returns the environment-aware S3 bucket name for a given data lake layer.

    Examples:
        - ENV="dev",  layer="bronze" -> "dev-lakehouse-bronze"
        - ENV="prod", layer="bronze" -> "prod-lakehouse-bronze"
        - ENV="dev",  layer="logs"   -> "dev-airflow-logs"

    Args:
        layer (str): The lakehouse layer identifier ('bronze', 'silver', 'gold', 'logs').

    Returns:
        str: The environment-prefixed S3 bucket name.

    Raises:
        ValueError: If an unrecognized layer identifier is provided.
    """
    normalized_layer = layer.strip().lower()

    # 2. Static mapping for standard layers vs. operational logs
    layer_suffix_map: Dict[str, str] = {
        "bronze": "lakehouse-bronze",
        "silver": "lakehouse-silver",
        "gold": "lakehouse-gold",
        "logs": "airflow-logs",
    }

    if normalized_layer not in layer_suffix_map:
        valid_layers = ", ".join(f"'{k}'" for k in layer_suffix_map.keys())
        raise ValueError(
            f"Invalid data lake layer '{layer}'. Valid choices are: {valid_layers}"
        )

    # 3. Construct environment-aware bucket name
    bucket_suffix = layer_suffix_map[normalized_layer]
    return f"{ENVIRONMENT}-{bucket_suffix}"