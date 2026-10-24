{#-
  Incremental by month (ADR 0007): hive table partitioned by pickup_month, strategy
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
    t.trip_id,
    t.vendor_id,
    v.vendor_name,
    t.pickup_at,
    t.dropoff_at,
    date(t.pickup_at) as pickup_date,
    hour(t.pickup_at) as pickup_hour,
    day_of_week(t.pickup_at) as pickup_dow,
    date_diff('second', t.pickup_at, t.dropoff_at) / 60.0 as trip_duration_min,
    t.passenger_count,
    t.trip_distance_miles,
    case when date_diff('second', t.pickup_at, t.dropoff_at) > 0
         then t.trip_distance_miles / (date_diff('second', t.pickup_at, t.dropoff_at) / 3600.0)
    end as avg_speed_mph,
    t.pickup_zone_id,
    pz.borough as pickup_borough,
    pz.zone_name as pickup_zone,
    t.dropoff_zone_id,
    dz.borough as dropoff_borough,
    dz.zone_name as dropoff_zone,
    t.payment_type,
    case t.payment_type
        when 0 then 'flex_fare' when 1 then 'credit_card' when 2 then 'cash'
        when 3 then 'no_charge' when 4 then 'dispute' when 5 then 'unknown'
        when 6 then 'voided'
    end as payment_type_name,
    t.fare_amount,
    t.tip_amount,
    t.tolls_amount,
    t.total_amount,
    case when t.fare_amount > 0 then t.tip_amount / t.fare_amount end as tip_pct,
    -- partition column: must stay last
    format('%04d-%02d', t.src_year, t.src_month) as pickup_month
from {{ ref('stg_trips') }} t
left join {{ ref('dim_zone') }} pz on t.pickup_zone_id = pz.zone_id
left join {{ ref('dim_zone') }} dz on t.dropoff_zone_id = dz.zone_id
left join {{ ref('dim_vendor') }} v
    on t.vendor_id = v.vendor_id and v.dbt_valid_to is null
{% if run_month is not none and not flags.FULL_REFRESH %}
-- src_year/src_month filter lets Athena prune raw partitions (see stg_trips dedup window)
where t.src_year = {{ run_year }}
  and t.src_month = {{ (run_month | string)[5:7] | int }}
{% endif %}
