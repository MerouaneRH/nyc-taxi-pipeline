{#-
  Incremental by month (ADR 0007), same pattern as fct_trips: hive table partitioned by pickup_month, strategy
  insert_overwrite. `--vars '{run_month: "2024-02"}'` rebuilds only that partition;
  without run_month an incremental run rewrites every partition in range; --full-refresh
  drops and rebuilds the table.
-#}
{%- set run_month = var('run_month', none) -%}
{%- if run_month is not none -%}
  {%- if not modules.re.match('^[0-9]{4}-(0[1-9]|1[0-2])$', run_month | string) -%}
    {{ exceptions.raise_compiler_error("run_month must match YYYY-MM, got: " ~ run_month) }}
  {%- endif -%}
  {%- set run_year = (run_month | string)[:4] | int -%}
  {%- if run_year < var('min_year') | int or run_year > var('max_year') | int -%}
    {{ exceptions.raise_compiler_error("run_month " ~ run_month ~ " is outside min_year..max_year") }}
  {%- endif -%}
{%- endif -%}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['pickup_month'],
    table_type='hive',
    format='parquet',
) }}

select
    pickup_date,
    pickup_hour,
    pickup_dow,
    pickup_borough,
    payment_type_name,
    vendor_name,
    count(*) as trips,
    sum(total_amount) as total_revenue,
    sum(fare_amount) as fare_revenue,
    sum(tip_amount) as tip_revenue,
    sum(trip_distance_miles) as distance_miles_sum,
    sum(trip_duration_min) as duration_min_sum,
    sum(passenger_count) as passenger_sum,
    -- tips are only recorded for card payments: tip % = card_tip_revenue / card_fare_revenue
    sum(case when payment_type_name = 'credit_card' then fare_amount end) as card_fare_revenue,
    sum(case when payment_type_name = 'credit_card' then tip_amount end) as card_tip_revenue,
    to_hex(md5(to_utf8(concat_ws('|',
        cast(pickup_date as varchar), cast(pickup_hour as varchar),
        coalesce(pickup_borough, '~'), coalesce(payment_type_name, '~'),
        coalesce(vendor_name, '~'))))) as row_key,
    -- partition column: must stay last
    pickup_month
from {{ ref('fct_trips') }}
{% if run_month is not none and not flags.FULL_REFRESH %}
where pickup_month = '{{ run_month }}'
{% endif %}
group by pickup_month, pickup_date, pickup_hour, pickup_dow, pickup_borough, payment_type_name, vendor_name
