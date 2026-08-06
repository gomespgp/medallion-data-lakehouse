import os


def get_db_conn(name: str) -> str:
    """
    Retrieves a database connection string from environment variables.
    
    Checks environment variables in the following order of precedence:
      1. AIRFLOW_CONN_{NAME}  (e.g., AIRFLOW_CONN_ECOMMERCE_DB)
      2. {NAME}_CONN          (e.g., ECOMMERCE_DB_CONN)
      3. {NAME}               (e.g., ECOMMERCE_DB)

    Args:
        name (str): The database name or identifier (e.g. "ecommerce_db").

    Returns:
        str: The target database connection URI.

    Raises:
        ValueError: If no matching environment variable is set.
    """
    raw_name = name.strip().upper()

    # Strip existing prefixes/suffixes to get clean base key (e.g., "ECOMMERCE_DB")
    base_key = raw_name
    if base_key.startswith("AIRFLOW_CONN_"):
        base_key = base_key[len("AIRFLOW_CONN_") :]
    if base_key.endswith("_CONN"):
        base_key = base_key[:-5]

    # Candidates to search in order of preference
    candidates = [
        f"AIRFLOW_CONN_{base_key}",
        f"{base_key}_CONN",
        base_key,
    ]

    for key in candidates:
        conn_str = os.getenv(key)
        if conn_str:
            return conn_str

    formatted_candidates = ", ".join(f"'{k}'" for k in candidates)
    raise ValueError(
        f"Missing database connection variable for '{name}'. "
        f"Checked environment keys: {formatted_candidates}"
    )