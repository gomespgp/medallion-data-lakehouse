{{ config(
    materialized='external',
    format='parquet',
    location='s3://dev-lakehouse-silver/postgres_ecommerce_db/users/stg_users.parquet'
) }}

with source as (
    select * 
    from read_parquet('s3://dev-lakehouse-bronze/postgres_ecommerce_db/users/**/*.parquet', hive_partitioning=1)
),
renamed as (
    select
        user_id,
        full_name,
        email,
        signup_date,
        -- Example cleaning / standardization
        lower(email) as clean_email,
        cast(signup_date as date) as signup_day
    from source
)

select * from renamed