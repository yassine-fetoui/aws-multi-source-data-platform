-- macros/generate_surrogate_key.sql
-- Generates a consistent surrogate key from one or more columns
-- Usage: {{ generate_surrogate_key(['customer_id', 'order_id']) }}

{% macro generate_surrogate_key(field_list) %}

    md5(
        {%- for field in field_list %}
            coalesce(cast({{ field }} as varchar), 'NULL')
            {%- if not loop.last %} || '|' || {% endif %}
        {%- endfor %}
    )

{% endmacro %}


-- macros/safe_divide.sql
-- Safe division that returns NULL instead of division-by-zero error
-- Usage: {{ safe_divide('numerator', 'denominator') }}

{% macro safe_divide(numerator, denominator, default=None) %}

    case
        when {{ denominator }} = 0 or {{ denominator }} is null
        then {{ default if default is not none else 'null' }}
        else {{ numerator }}::decimal(20, 6) / {{ denominator }}
    end

{% endmacro %}


-- macros/date_spine.sql
-- Generates a spine of dates between two dates
-- Useful for filling gaps in time-series data
-- Usage: {{ date_spine(start_date='2025-01-01', end_date='current_date') }}

{% macro date_spine(start_date, end_date) %}

    with date_spine as (
        select
            dateadd('day', seq4(), '{{ start_date }}'::date) as date_day
        from table(generator(rowcount => datediff('day', '{{ start_date }}'::date, {{ end_date }}) + 1))
    )
    select
        date_day,
        extract(year  from date_day) as year,
        extract(month from date_day) as month,
        extract(day   from date_day) as day,
        extract(dow   from date_day) as day_of_week,
        extract(week  from date_day) as week_of_year,
        date_trunc('week',  date_day) as week_start,
        date_trunc('month', date_day) as month_start,
        date_trunc('quarter', date_day) as quarter_start,
        case extract(dow from date_day)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name,
        case when extract(dow from date_day) in (0, 6) then false else true end as is_weekday
    from date_spine

{% endmacro %}


-- macros/assert_row_count.sql
-- Post-run hook macro to assert minimum row count after a model runs
-- Blocks deployment if a model writes fewer rows than expected
-- Usage: add to model config: post_hook="{{ assert_row_count(min_rows=1) }}"

{% macro assert_row_count(min_rows=1) %}

    {% set row_count_query %}
        select count(1) as row_count from {{ this }}
    {% endset %}

    {% set results = run_query(row_count_query) %}

    {% if execute %}
        {% set row_count = results.columns[0].values()[0] %}
        {% if row_count < min_rows %}
            {{ exceptions.raise_compiler_error(
                "Zero-row assertion FAILED for " ~ this ~ ": "
                ~ row_count ~ " rows found, minimum " ~ min_rows ~ " expected. "
                ~ "This model wrote zero rows — blocking deployment."
            ) }}
        {% endif %}
        {{ log("Row count assertion PASSED: " ~ this ~ " has " ~ row_count ~ " rows", info=True) }}
    {% endif %}

{% endmacro %}
