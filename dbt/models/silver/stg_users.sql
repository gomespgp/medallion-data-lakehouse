{{ config(
    materialized='external',
    format='parquet',
    location=get_s3_location('silver', 'postgres_ecommerce_db')
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