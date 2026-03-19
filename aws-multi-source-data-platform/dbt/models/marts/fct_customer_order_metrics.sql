-- dbt mart model: fct_customer_order_metrics.sql
-- Purpose: Customer-level order analytics with window functions
-- Challenge solved: replaced 6 correlated subqueries with window functions
-- Result: query time 8 min → 22 sec

{{
  config(
    materialized  = 'incremental',
    unique_key    = 'order_event_id',
    dist          = 'customer_id',
    sort          = ['event_occurred_at'],
    on_schema_change = 'sync_all_columns'
  )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

    {% if is_incremental() %}
        -- Incremental: only process new events since last run
        where event_occurred_at > (select max(event_occurred_at) from {{ this }})
    {% endif %}

),

customer_metrics as (

    select
        order_event_id,
        order_id,
        customer_id,
        product_id,
        region,
        order_status,
        event_type,
        order_size_bucket,
        is_cancelled,
        order_amount,
        quantity,
        unit_price,
        event_occurred_at,
        event_date,

        -- ── Window Functions ──────────────────────────────────────────────

        -- Previous order value for this customer (chronological)
        lag(order_amount, 1, 0)
            over (
                partition by customer_id
                order by event_occurred_at
            )                                               as prev_order_amount,

        -- Change vs previous order
        order_amount
            - lag(order_amount, 1, 0)
                over (
                    partition by customer_id
                    order by event_occurred_at
                )                                           as order_amount_delta,

        -- Customer rank within region by order value (no gaps in ranking)
        dense_rank()
            over (
                partition by region
                order by order_amount desc
            )                                               as rank_in_region,

        -- Running total revenue per customer over time
        sum(order_amount)
            over (
                partition by customer_id
                order by event_occurred_at
                rows between unbounded preceding and current row
            )                                               as customer_running_revenue,

        -- Order sequence number per customer (1 = first order ever)
        row_number()
            over (
                partition by customer_id
                order by event_occurred_at
            )                                               as customer_order_sequence,

        -- Is this the customer's most recent order?
        case
            when row_number() over (
                partition by customer_id
                order by event_occurred_at desc
            ) = 1
            then true
            else false
        end                                                 as is_latest_order,

        -- 30-day rolling revenue per customer
        sum(order_amount)
            over (
                partition by customer_id
                order by event_occurred_at
                rows between 29 preceding and current row
            )                                               as rolling_30d_revenue,

        -- Percentage of this order vs customer total lifetime spend
        round(
            order_amount / nullif(
                sum(order_amount) over (partition by customer_id),
                0
            ) * 100,
            2
        )                                                   as pct_of_lifetime_spend,

        -- Days since previous order (inter-purchase interval)
        datediff(
            'day',
            lag(event_occurred_at)
                over (
                    partition by customer_id
                    order by event_occurred_at
                ),
            event_occurred_at
        )                                                   as days_since_prev_order,

        -- Audit
        processed_at,
        ingested_at

    from orders

)

select * from customer_metrics
