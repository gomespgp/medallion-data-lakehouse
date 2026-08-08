{{ config(
    materialized='external',
    format='parquet',
    location=get_s3_location('gold', 'postgres_ecommerce_db')
) }}

with
    orders as (
        select * from {{ source('silver', 'stg_orders') }}
    ),

    daily_metrics as (
        select
            order_date,
            year(order_date) as order_year,
            month(order_date) as order_month,
            day(order_date) as order_day,
            count(distinct order_id) as total_orders,
            count(distinct user_id) as unique_buyers,
            count(case when clean_order_status = 'COMPLETED' then 1 end) as completed_orders,
            count(case when clean_order_status = 'CANCELLED' then 1 end) as cancelled_orders,
            count(case when clean_order_status = 'PENDING' then 1 end) as pending_orders,
            coalesce(sum(case when clean_order_status = 'COMPLETED' then order_amount else 0 end), 0.00) as total_revenue,
            coalesce(avg(case when clean_order_status = 'COMPLETED' then order_amount end), 0.00) as average_order_value
        from orders
        group by 1, 2, 3, 4
    )

    select * from daily_metrics