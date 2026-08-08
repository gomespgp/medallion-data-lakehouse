{{ config(
    materialized='external',
    format='parquet',
    location=get_s3_location('silver', 'postgres_ecommerce_db')
) }}

with
    source as (
        select * 
        from {{ source('bronze', 'orders') }}
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