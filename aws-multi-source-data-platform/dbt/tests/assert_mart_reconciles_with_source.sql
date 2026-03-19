-- dbt singular test: assert_mart_reconciles_with_source.sql
-- Reconciliation test: Gold mart total must match Bronze source-of-truth
-- Variance above 0.1% fails CI — catches silent data loss between layers

with bronze_total as (

    select
        count(1)          as bronze_row_count,
        sum(total_amount) as bronze_revenue
    from {{ source('raw', 'orders_raw') }}
    where ingested_at >= dateadd('day', -1, current_date)

),

gold_total as (

    select
        count(1)           as gold_row_count,
        sum(order_amount)  as gold_revenue
    from {{ ref('fct_customer_order_metrics') }}
    where ingested_at >= dateadd('day', -1, current_date)

),

reconciliation as (

    select
        b.bronze_row_count,
        g.gold_row_count,
        b.bronze_revenue,
        g.gold_revenue,

        -- Row count variance %
        abs(b.bronze_row_count - g.gold_row_count) * 100.0
            / nullif(b.bronze_row_count, 0)         as row_variance_pct,

        -- Revenue variance %
        abs(b.bronze_revenue - g.gold_revenue) * 100.0
            / nullif(b.bronze_revenue, 0)           as revenue_variance_pct

    from bronze_total b
    cross join gold_total g

)

-- Return rows only when variance exceeds threshold — causes test to FAIL
select *
from reconciliation
where
    row_variance_pct     > 0.1    -- more than 0.1% row count difference
    or revenue_variance_pct > 0.1 -- more than 0.1% revenue difference
