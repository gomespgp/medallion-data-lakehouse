{{ config(
    materialized='external',
    format='parquet',
    location='s3://dev-lakehouse-gold/postgres_ecommerce_db/dim_users/dim_users.parquet'
) }}

with users as (
    select * from {{ ref('stg_users') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

user_orders_aggregated as (
    select
        user_id,
        count(order_id) as total_orders,
        count(case when clean_order_status = 'COMPLETED' then 1 end) as completed_orders,
        coalesce(sum(case when clean_order_status = 'COMPLETED' then order_amount else 0 end), 0) as lifetime_spend,
        min(created_at) as first_order_date,
        max(created_at) as most_recent_order_date
    from orders
    group by 1
),

final as (
    select
        u.user_id,
        u.full_name,
        u.clean_email as email,
        u.signup_date,
        u.signup_day,
        coalesce(uo.total_orders, 0) as total_orders,
        coalesce(uo.completed_orders, 0) as completed_orders,
        coalesce(uo.lifetime_spend, 0.00) as lifetime_spend,
        uo.first_order_date,
        uo.most_recent_order_date,
        case 
            when coalesce(uo.lifetime_spend, 0) >= 200 then 'VIP'
            when coalesce(uo.lifetime_spend, 0) > 0 then 'Active'
            else 'Inactive'
        end as customer_segment
    from users u
    left join user_orders_aggregated uo
        on u.user_id = uo.user_id
)

select * from final