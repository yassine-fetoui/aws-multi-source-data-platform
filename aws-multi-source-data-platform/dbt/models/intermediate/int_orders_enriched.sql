-- dbt intermediate model: int_orders_enriched.sql
-- Joins orders with customers and products — business logic layer
-- No window functions here — pure enrichment and classification

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

enriched as (

    select
        -- Order keys
        o.order_event_id,
        o.order_id,
        o.customer_id,
        o.product_id,

        -- Customer attributes at time of order
        c.customer_name,
        c.segment                           as customer_segment,
        c.region                            as customer_region,
        c.account_type,
        c.country_code,

        -- Order measures
        o.order_amount,
        o.quantity,
        o.unit_price,
        o.order_status,
        o.event_type,
        o.order_size_bucket,
        o.is_cancelled,

        -- Business classification
        case
            when o.order_amount >= 500 then 'enterprise'
            when o.order_amount >= 200 then 'commercial'
            when o.order_amount >= 50  then 'standard'
            else 'micro'
        end                                 as revenue_tier,

        -- Time dimensions
        o.event_occurred_at,
        o.event_date,
        date_trunc('week',  o.event_date)   as event_week,
        date_trunc('month', o.event_date)   as event_month,
        date_trunc('year',  o.event_date)   as event_year,
        extract(dow from o.event_occurred_at) as day_of_week,
        extract(hour from o.event_occurred_at) as hour_of_day,

        -- Audit
        o.processed_at,
        o.ingested_at

    from orders o
    left join customers c
        on o.customer_id = c.customer_id

)

select * from enriched
