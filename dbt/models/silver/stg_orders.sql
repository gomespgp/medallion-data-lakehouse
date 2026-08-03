{{ config(
    materialized='external',
    format='parquet',
    location='s3://dev-lakehouse-silver/postgres_ecommerce_db/orders/stg_orders.parquet'
) }}

with source as (
    select * 
    from read_parquet('s3://dev-lakehouse-bronze/postgres_ecommerce_db/orders/**/*.parquet', hive_partitioning=1)
),
renamed as (
    select
        order_id,
        user_id,
        order_status,
        order_amount,
        created_at,
        -- Example cleaning / standardization
        upper(order_status) as clean_order_status,
        cast(created_at as date) as order_date
    from source
)

select * from renamed