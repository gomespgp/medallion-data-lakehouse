import os
import io
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine
from minio import Minio

POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
POSTGRES_HOST = 'postgres_source'
POSTGRES_PORT = '5432'
POSTGRES_DB = os.getenv('POSTGRES_DB', 'ecommerce')

MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')

BRONZE_BUCKET = 'dev-lakehouse-bronze'
SOURCE_DOMAIN = 'postgres_ecommerce_db'

# ---------------------------------------------------------------------------
# Python Callable
# ---------------------------------------------------------------------------
def extract_table_to_minio_parquet(table_name: str, **kwargs):
    """
    Extracts a table from PostgreSQL, converts to Parquet,
    and uploads to MinIO (Bronze Layer) using standard Hive partitioning.
    """
    # Extract logical execution date components for standard Hive partitioning
    logical_date = kwargs.get('logical_date', datetime.now())
    year = logical_date.strftime('%Y')
    month = logical_date.strftime('%m')
    day = logical_date.strftime('%d')

    print(f"--- Extraction: {table_name} (Partition: year={year}/month={month}/day={day}) ---")
    
    # 1. Read Data from Source DB
    db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    engine = create_engine(db_url)
    
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, con=engine)
    print(f"Extracted {len(df)} rows from '{table_name}'.")

    # 2. Convert to Parquet Stream
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
    parquet_buffer.seek(0)

    # 3. Connect to MinIO
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    if not minio_client.bucket_exists(BRONZE_BUCKET):
        minio_client.make_bucket(BRONZE_BUCKET)

    # 4. Domain-Driven Hive S3 Key
    destination_path = (
        f"{SOURCE_DOMAIN}/{table_name}/"
        f"year={year}/month={month}/day={day}/{table_name}.parquet"
    )
    
    minio_client.put_object(
        bucket_name=BRONZE_BUCKET,
        object_name=destination_path,
        data=parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    
    print(f"Successfully uploaded to s3://{BRONZE_BUCKET}/{destination_path}")