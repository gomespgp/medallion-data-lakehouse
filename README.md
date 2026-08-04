# 🌊 Local Open Data Lakehouse

An end-to-end local Data Lakehouse platform leveraging modern data stack tooling to ingest transactional data, store it in open Parquet format on object storage, transform it into star schema models using dbt, and serve interactive analytics in Superset.

---

## 📌 Features

- **ELT Architecture:** Incremental extraction from PostgreSQL OLTP into object storage before executing transformations.
- **Medallion Storage:** Structured object storage layout in MinIO (Bronze -> Silver -> Gold).
- **Open Standards:** Compressed Parquet file formats managed with DuckDB and dbt-duckdb.
- **Orchestration:** Automated daily runs scheduled via Apache Airflow.
- **BI & Analytics:** Apache Superset dashboards backed by DuckDB's high-performance query engine.

---

## 🚀 Quickstart

### 1. Launch Services via Docker Compose
Ensure Docker Desktop is running, then spin up the infrastructure:

make up
# or: docker compose -f .docker/docker-compose.yaml up -d

### 2. Service Access Points

| Service | Port | Default Credentials | Description |
| :--- | :--- | :--- | :--- |
| **Airflow UI** | `8080` | `admin` / `admin` | Pipeline Orchestration |
| **MinIO Console** | `9001` | `minioadmin` / `minioadmin` | Data Lake S3 Storage |
| **Apache Superset** | `8088` | `admin` / `admin` | Business Intelligence & SQL Lab |
| **PostgreSQL (Source)** | `5432` | `postgres` / `postgres` | Operational Source DB (`ecommerce`) |

---

## 💻 Local DAG Development & Environment Setup

To get full IDE support (autocomplete, linting, and type checking) when developing Airflow DAGs locally, set up a Python 3.10 virtual environment mapped to the container's dependencies.

### 1. Setup Local Virtual Environment

Choose one of the following methods to create and activate your virtual environment:

#### Option A: Using `uv` (Recommended - Ultra Fast)

```bash
# Create virtual environment with Python 3.10
uv venv .venv --python 3.10

# Activate virtual environment
# On Windows:
.venv\Scripts\activate
# On Linux/macOS:
source .venv/bin/activate

# Install dependencies using Airflow constraint
uv pip install "apache-airflow==2.10.0" --constraint "[https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-3.10.txt](https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-3.10.txt)"
uv pip install -r .docker/config/requirements.txt
```

#### Option B: Using Standard `venv`

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows:
.venv\Scripts\activate
# On Linux/macOS:
source .venv/bin/activate

# Upgrade pip & install dependencies using Airflow constraint
python -m pip install --upgrade pip
python -m pip install "apache-airflow==2.10.0" --constraint "[https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-3.10.txt](https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-3.10.txt)"
python -m pip install -r .docker/config/requirements.txt
```

### 2. Configure Editor
Point your IDE (VS Code / PyCharm) interpreter to `.venv/Scripts/python.exe` (Windows) or `.venv/bin/python` (Linux/macOS).

### 3. Test DAGs Locally via CLI

Validate DAG syntax and imports:
python airflow/dags/ecommerce_ingestion/dag.py

Dry-run a specific task in isolation (Execution Date format: YYYY-MM-DD):
airflow tasks test ecommerce_ingestion_dag ingest_users_to_bronze 2026-08-01

---

## 📂 Repository Structure

```text
local-data-lakehouse/
|-- .docker/
|   |-- config/
|   |   `-- requirements.txt          # Single source of truth for Airflow python packages
|   |-- docker-compose.yaml           # Infrastructure definition
|   `-- .env                          # Local environment variables
|-- airflow/
|   `-- dags/
|       `-- ecommerce_ingestion/      # Ingestion DAG module
|           `-- dag.py
|-- dbt/                              # dbt transformation models and DuckDB configs
|-- ARCHITECTURE.md                   # Detailed technical design & system flow
`-- README.md
```