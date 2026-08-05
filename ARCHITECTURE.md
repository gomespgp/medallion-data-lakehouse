# Architecture & Technical Design

This document details the architecture, design choices, infrastructure components, and storage layout of the local Open Data Lakehouse platform.

---

## System Overview

The platform implements an ELT (Extract, Load, Transform) pattern to ingest transactional operational data, store it in open file formats on S3-compatible object storage, transform it into dimensional models, and serve it to analytics and visualization tools.
```mermaid
flowchart LR
    subgraph Source [Operational Layer]
        PG[("PostgreSQL\n(ecommerce DB)")]
    end

    subgraph Storage [MinIO Object Storage]
        Bronze[("s3://dev-lakehouse-bronze")]
        Silver[("s3://dev-lakehouse-silver")]
        Gold[("s3://dev-lakehouse-gold")]
    end

    subgraph Serving [Analytics Layer]
        DuckDB[("DuckDB Query Engine")]
        Superset["Apache Superset\n(Dashboards & SQL Lab)"]
    end

    PG -->|"1. Ingest Raw Parquet"| Airflow["Apache Airflow"]
    Airflow -->|"2. Write Parquet"| Bronze
    Bronze -->|"3. Transform SQL"| dbt["dbt Engine"]
    dbt -->|"4. Staging Models"| Silver
    dbt -->|"5. Star Schemas"| Gold
    Gold -->|"6. Direct Query"| DuckDB
    DuckDB -->|"7. Visualize"| Superset
```
---

## Tech Stack & Components

- Source DB (OLTP): PostgreSQL 16-alpine (Operational database named `ecommerce` holding raw transactional tables)
- Orchestration: Apache Airflow 2.10.0 (Schedules and orchestrates data ingestion pipelines and dbt transformations)
- Data Lake Storage: MinIO RELEASE.2024-06-13 (S3-compatible local object storage hosting multi-bucket Medallion architecture and operational logs)
- Transformation: dbt 1.8+ (Handles data modeling, staging, deduplication, star schema creation, and data quality testing)
- Query Engine: DuckDB Latest (Embedded, high-performance OLAP SQL engine querying compressed Parquet files directly from MinIO)
- BI & Analytics: Apache Superset 6.0.0 (Visualization layer and ad-hoc SQL editor querying clean data models via DuckDB)

---

## End-to-End Data Lifecycle

1. Extraction (Airflow): Dynamic Airflow DAGs read DAG-specific `config.yaml` files, extract tables incrementally from PostgreSQL, and format date partitions.
2. Storage (MinIO - Bronze Layer): Extracted records are serialized into snappy-compressed Parquet files and uploaded to `s3://dev-lakehouse-bronze/` using domain-driven Hive paths.
3. Execution Logging (MinIO - Operational): Airflow streams task execution logs directly to `s3://dev-airflow-logs/` via the AWS S3 Provider, keeping containers stateless.
4. Transformation (dbt + DuckDB): Airflow triggers dbt runs. The dbt-duckdb engine queries raw Parquet files from Bronze, applies transformations, and materializes output tables into `dev-lakehouse-silver` and `dev-lakehouse-gold` buckets.
5. Consumption (Superset): Apache Superset connects to DuckDB as its query engine, providing interactive dashboards and SQL lab capabilities without exposing underlying S3 credentials.

---

## Storage Topology & Architectural Design Decisions

### 1. Dedicated Multi-Bucket Medallion Layout
Rather than storing all data in a single monolithic bucket with subfolders, storage is split across dedicated buckets. This enforces strict environment boundaries, simplifies IAM/access policies, and enables dedicated lifecycle management.

- `dev-lakehouse-bronze`: Immutable raw data landing layer.
- `dev-lakehouse-silver`: Cleaned, typed, and deduplicated staging data.
- `dev-lakehouse-gold`: Business-ready Star Schemas (Fact and Dimension tables).
- `dev-airflow-logs`: Isolated operational logs for Airflow task execution.

### 2. Domain-Driven S3 Pathing (Decoupled from Orchestration)
S3 key structures follow a domain-driven naming convention. Paths explicitly reference the data domain and source system rather than the orchestrator tool (e.g., avoiding `airflow/dags/` prefixes). This ensures that migrating or upgrading orchestration engines does not require costly data path relocations.

S3 Path Pattern:
s3://[bucket-name]/[source_system_domain]/[table_name]/[partition_path]/[table_name].parquet

Example (Bronze Layer):
s3://dev-lakehouse-bronze/postgres_ecommerce_db/users/year=2026/month=08/day=01/users.parquet

### 3. Hive-Style Key-Value Partitioning
Partition folders follow standard Hive key-value formatting (`year=YYYY/month=MM/day=DD`). This key-value structure allows analytical engines (DuckDB, Spark, Trino, Athena) to perform automatic partition pruning when executing SQL queries over S3.

---

## Medallion Lakehouse Structure

```mermaid
graph TD
    subgraph Bronze ["🥉 BRONZE LAYER (s3://dev-lakehouse-bronze)"]
        direction TB
        B1["postgres_ecommerce_db/users/year=YYYY/month=MM/day=DD/users.parquet"]
        B2["postgres_ecommerce_db/orders/year=YYYY/month=MM/day=DD/orders.parquet"]
    end

    subgraph Silver ["🥈 SILVER LAYER (s3://dev-lakehouse-silver)"]
        direction TB
        S1["postgres_ecommerce_db/stg_users.parquet"]
        S2["postgres_ecommerce_db/stg_orders.parquet"]
    end

    subgraph Gold ["🥇 GOLD LAYER (s3://dev-lakehouse-gold)"]
        direction TB
        G1["dim_customers.parquet"]
        G2["fact_sales.parquet"]
    end

    subgraph Logs ["📋 OPERATIONAL LOGS (s3://dev-airflow-logs)"]
        L1["dag_id=ecommerce_ingestion/run_id=.../task_id=.../attempt=1.log"]
    end

    Bronze -->|"dbt Clean & Deduplicate"| Silver
    Silver -->|"dbt Model & Aggregate"| Gold
```
---

## Orchestration & Framework Architecture (utils/dag_factory.py)

The platform leverages a centralized DAG Factory pattern powered by Pydantic v2 and Airflow Datasets. DAG definitions are completely decoupled from python boilerplate code and driven by clean, declarative YAML configurations.

### Directory Layout

```bash
dags/
├── utils/
│   ├── __init__.py
│   └── dag_factory.py        # Centralized factory logic & Pydantic models
├── dbt_run/
│   ├── src/
│   │   ├── dag/
│   │   │   ├── config.yaml   # Global base DAG configuration
│   │   │   └── models.py     # Pydantic domain models
│   ├── configs/
│   │   ├── silver.yaml       # Asset-specific configuration
│   │   └── gold.yaml         # Asset-specific configuration
│   └── _dag_dbt_run.py       # Dynamic entry point
```

### Factory Mechanisms

#### 1. Single DAG Generation (create_dag())
Used for single-purpose DAGs (e.g., operational database fetch). Loads a single YAML configuration file located at `DAG_PATH/src/dag/config.yaml`, validates settings via `AirflowConfigModel`, and returns:
- `dag`: An initialized airflow.DAG object.
- `dag_config`: A validated domain configuration (parsed into a Pydantic model or raw dict).
- `airflow_config`: The resolved Airflow metadata.

#### 2. Dynamic Multi-DAG Generation (create_dynamic_dags())
Used for generating decoupled pipeline layers dynamically (e.g., separate Silver and Gold transformation DAGs).
- Hierarchical Config Merging: Reads shared settings from `DAG_PATH/src/dag/config.yaml` and deep-merges them with asset-specific configs in `DAG_PATH/configs/*.yaml`. Asset configs override global parameters.
- Automatic Asset Discovery: Scans `configs/*.yaml` and creates one DAG per configuration file.
- Returns: A list of tuples containing (`dag`, `asset_name`, `dag_config`,` airflow_config`).

### Event-Driven Scheduling with Airflow Datasets

Pipelines communicate across layers using event-driven Airflow Datasets instead of rigid cron execution times or external trigger sensors:

```mermaid
graph LR
    FETCH["ecommerce_db_fetch"]
    SILVER["dbt_run_silver"]
    GOLD["dbt_run_gold"]

    FETCH -->|"Emits: Dataset('ecommerce_db_fetch.ingest_orders_to_bronze')"| SILVER
    SILVER -->|"Emits: Dataset('dbt_run_silver.run_dbt_model')"| GOLD
```

1. Inbound Schedule: When a YAML config specifies `dependencies: [{dag_id: "...", task_id: "..."}]`, dag_factory converts them directly into `schedule=[Dataset(...)]`.
2. Outbound Trigger: Tasks emit explicit `outlets=[Dataset(f"{dag.dag_id}.{task_id}")]` upon successful execution, notifying Airflow to immediately trigger downstream DAGs listening for that dataset.

---

## Integration Note: Connecting with External Services (e.g., Go E-Commerce API)

While this project includes a dedicated PostgreSQL container (`postgres_source`) for standalone execution, it is designed to seamlessly integrate with external local microservices, such as an E-Commerce API written in Go.

### How to Connect via Shared Docker Network

Instead of reading from static seed databases, configure Airflow to ingest live transactional data generated by an external Go API database by attaching services to a shared Docker bridge network.

1. Define the Shared Network in `.docker/docker-compose.yaml`:
```yaml
networks:
  default:
    name: ecommerce_shared_network
    external: true
```

2. Update Airflow Database Connection:
Set `POSTGRES_HOST` and `POSTGRES_DB` to target the external API database container (e.g., `go_api_postgres:5432` / `ecommerce`).