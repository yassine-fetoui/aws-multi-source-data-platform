-- dbt staging model: stg_orders.sql
-- Source: Curated S3 zone loaded into Redshift via COPY INTO
-- Purpose: Clean, type-cast, and rename raw columns — no business logic here

with source as (

    select * from {{ source('curated', 'orders_curated') }}

),

renamed as (

    select
        -- Keys
        event_id                                    as order_event_id,
        order_id,
        customer_id,
        product_id,

        -- Measures
        quantity::integer                           as quantity,
        unit_price::decimal(12, 2)                  as unit_price,
        total_amount::decimal(12, 2)                as order_amount,

        -- Categorical
        upper(trim(status))                         as order_status,
        upper(trim(event_type))                     as event_type,
        upper(trim(region))                         as region,
        upper(trim(order_size_bucket))              as order_size_bucket,

        -- Booleans
        is_cancelled::boolean                       as is_cancelled,

        -- Timestamps
        event_timestamp_ts::timestamp               as event_occurred_at,
        event_date::date                            as event_date,
        processed_at::timestamp                     as processed_at,
        ingested_at_ts::timestamp                   as ingested_at,

        -- Audit
        pipeline_version

    from source

),

-- Remove any records that slipped through upstream validation
validated as (

    select *
    from renamed
    where
        order_event_id  is not null
        and order_id    is not null
        and customer_id is not null
        and order_amount > 0
        and event_occurred_at is not null
        and order_status in (
            'PENDING', 'CONFIRMED', 'PROCESSING',
            'SHIPPED', 'DELIVERED', 'CANCELLED'
        )

)

select * from validated
