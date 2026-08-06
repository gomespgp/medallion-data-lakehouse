{{ config(
    materialized='external',
    format='parquet',
    location='s3://dev-lakehouse-silver/postgres_ecommerce_db/users/' ~ var('partition_path') ~ '/stg_users.parquet'
) }}

with
    source as (
        select * 
        from {{ source('bronze', 'users') }}
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