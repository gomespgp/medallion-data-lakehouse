# Architecture & Technical Design

This document details the architecture, design choices, infrastructure components, and storage layout of the local Open Data Lakehouse platform.

---

## System Overview

The platform implements an ELT (Extract, Load, Transform) pattern to ingest transactional operational data, store it in open file formats on S3-compatible object storage, transform it into dimensional models, and serve it to analytics and visualization tools.

+------------------------+      +------------------------+      +------------------------+
|  Operational Database  | ---> |   S3 Object Storage    | ---> | Analytical Engine & BI |
|  (PostgreSQL OLTP)     |      |   (MinIO Data Lake)    |      |  (DuckDB + Superset)   |
+------------------------+      +------------------------+      +------------------------+
            |                               |                               ^
            |                               |                               |
            +--------- (Airflow) -----------+------------- (dbt) ------------+

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

s3://dev-lakehouse-bronze/                # Raw, immutable Parquet files straight from source databases
`-- postgres_ecommerce_db/
    |-- users/
    |   `-- year=2026/month=08/day=01/users.parquet
    `-- orders/
        `-- year=2026/month=08/day=01/orders.parquet

s3://dev-lakehouse-silver/                # Cleaned, typed, and deduplicated staging models
`-- postgres_ecommerce_db/
    |-- stg_users/
    `-- stg_orders/

s3://dev-lakehouse-gold/                  # Production-ready Star Schemas (Fact & Dimension tables)
|-- dim_customers/
`-- fact_sales/

s3://dev-airflow-logs/                    # System logs streamed directly from Airflow containers
`-- dag_id=ecommerce_ingestion/
    `-- run_id=scheduled__2026-08-01T00:00:00+00:00/
        `-- task_id=ingest_users_to_bronze/
            `-- attempt=1.log

---

## Orchestration & Modular DAG Architecture

DAGs are organized using a folder-per-DAG structure (`airflow/dags/[dag_name]/`), housing both code (`dag.py`) and configuration (`config.yaml`).

Key Design Features:
- Configuration-Driven: Task generation and schedules are defined inside `config.yaml` rather than hardcoded in Python.
- Dynamic Execution Date Partitioning: Uses Airflow's `logical_date` context (`ds` / `year`, `month`, `day`) instead of `datetime.now()` to guarantee execution idempotency during historical backfills.
- Environment Sync: Packages are managed in `.docker/config/requirements.txt` as a single source of truth for both Docker containers and local development `.venv` environments.

---

## Integration Note: Connecting with External Services (e.g., Go E-Commerce API)

While this project includes a dedicated PostgreSQL container (`postgres_source`) for standalone execution, it is designed to seamlessly integrate with external local microservices, such as an E-Commerce API written in Go.

### How to Connect via Shared Docker Network

Instead of reading from static seed databases, configure Airflow to ingest live transactional data generated by an external Go API database by attaching services to a shared Docker bridge network.

1. Define the Shared Network in `.docker/docker-compose.yaml`:
networks:
  default:
    name: ecommerce_shared_network
    external: true

2. Update Airflow Database Connection:
Set `POSTGRES_HOST` and `POSTGRES_DB` to target the external API database container (e.g., `go_api_postgres:5432` / `ecommerce`).