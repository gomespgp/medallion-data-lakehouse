import io
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

from common_config import get_db_conn, get_s3_bucket
from commons import AwsS3

S3_BASE_KEY = "postgres_ecommerce_db"


def extract_table_to_minio_parquet(table_name: str, **kwargs):
    """
    Extracts a table from PostgreSQL, converts it to Parquet,
    and uploads to MinIO (Bronze Layer) using standard Hive partitioning.
    """
    execution_date = kwargs.get("data_interval_end").strftime("year=%Y/month=%m/day=%d")

    print(f"--- Extraction: {table_name} (Partition: {execution_date}) ---")

    # 1. Resolve DB Connection via get_db_conn helper
    db_conn_str = get_db_conn("ecommerce_db")
    engine = create_engine(db_conn_str)

    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, con=engine)
    print(f"Extracted {len(df)} rows from '{table_name}'.")

    # 2. Convert to Parquet Stream
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow", compression="snappy")

    # 3. Domain-Driven Hive S3 Key Path
    destination_path = f"{S3_BASE_KEY}/{table_name}/{execution_date}/{table_name}.parquet"

    # 4. Resolve Bucket & Upload
    bronze_bucket = get_s3_bucket("bronze")
    s3_client = AwsS3()

    s3_uri = s3_client.upload_stream(
        bucket_name=bronze_bucket,
        object_path=destination_path,
        buffer=parquet_buffer,
    )

    print(f"Successfully uploaded to {s3_uri}")