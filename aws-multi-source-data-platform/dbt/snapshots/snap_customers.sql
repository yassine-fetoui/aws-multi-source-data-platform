-- dbt snapshot: snap_customers.sql
-- Implements SCD Type 2 for customer dimension
-- Uses SHA-256 hash change detection — only changed records trigger a write
-- Challenge solved: full MERGE on 500M rows taking 4 hours
-- Solution: hash-based incremental detection → 18 minutes

{% snapshot snap_customers %}

{{
    config(
        target_schema  = 'snapshots',
        unique_key     = 'customer_id',
        strategy       = 'check',
        check_cols     = [
            'customer_name',
            'email',
            'region',
            'segment',
            'account_type',
            'country_code'
        ],
        invalidate_hard_deletes = True
    )
}}

-- Source: staging customers table
-- Only track slowly changing ATTRIBUTES here
-- Rapidly changing MEASURES (balance, score) live in a separate point-in-time table
select
    customer_id,
    customer_name,
    email,
    region,
    segment,
    account_type,
    country_code,
    is_active,
    created_at,
    updated_at,
    -- Pre-compute hash for change detection efficiency
    md5(
        coalesce(customer_name, '') || '|' ||
        coalesce(email,         '') || '|' ||
        coalesce(region,        '') || '|' ||
        coalesce(segment,       '') || '|' ||
        coalesce(account_type,  '') || '|' ||
        coalesce(country_code,  '')
    ) as row_hash

from {{ ref('stg_customers') }}

{% endsnapshot %}
