-- dbt singular test: assert_no_negative_order_amounts.sql
-- Business logic test: no order amount should ever be negative or zero
-- Structural tests (not_null, unique) would pass even with -999 amounts
-- This test encodes domain knowledge — catching what schema tests cannot

select
    order_event_id,
    order_id,
    customer_id,
    order_amount,
    order_status,
    event_occurred_at
from {{ ref('fct_customer_order_metrics') }}
where
    order_amount <= 0

-- This query should return ZERO rows
-- If any rows are returned, the dbt test fails and CI blocks the deployment
