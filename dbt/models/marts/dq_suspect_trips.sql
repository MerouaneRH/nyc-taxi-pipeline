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


with flagged as (
    select
        *,
        trip_distance_miles >= {{ var('dq_distance_absurd_miles') }} as f_absurd,
        coalesce(avg_speed_mph >= {{ var('dq_speed_max_mph') }}, false)
            and not (trip_distance_miles >= {{ var('dq_distance_absurd_miles') }}) as f_speed,
        trip_duration_min * 60 < {{ var('dq_short_trip_seconds') }}
            and trip_distance_miles >= {{ var('dq_short_trip_min_miles') }} as f_ts,
        trip_distance_miles = 0 and total_amount >= {{ var('dq_zero_distance_min_fare') }} as f_zero_dist,
        trip_duration_min >= {{ var('dq_long_trip_minutes') }} as f_long,
        total_amount = 0 as f_zero_total
    from {{ ref('fct_trips') }}
    {% if run_month is not none and not flags.FULL_REFRESH %}
    where pickup_month = '{{ run_month }}'
    {% endif %}
),

coded as (
    -- array order = priority order
    select
        *,
        filter(array[
            if(f_absurd, 'DISTANCE_ABSURD'),
            if(f_speed, 'SPEED_IMPLAUSIBLE'),
            if(f_ts, 'TIMESTAMP_SUSPECT'),
            if(f_zero_dist, 'ZERO_DISTANCE_PAID'),
            if(f_long, 'LONG_DURATION'),
            if(f_zero_total, 'ZERO_TOTAL')
        ], x -> x is not null) as codes,
        -- timestamps-vs-distance blame of the speed rule depends on the duration
        trip_duration_min * 60 < {{ var('dq_short_trip_seconds') }} as short_trip
    from flagged
)

select
    trip_id,
    pickup_at,
    dropoff_at,
    trip_duration_min,
    trip_distance_miles,
    avg_speed_mph,
    passenger_count,
    fare_amount,
    tip_amount,
    total_amount,
    payment_type_name,
    vendor_name,
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,
    codes[1] as reason_code,
    array_join(codes, ',') as reason_codes,
    -- primary code decides the field, unless distance and timestamps rules both fired
    case
        when (f_absurd or f_zero_dist or (f_speed and not short_trip))
         and (f_ts or f_long or (f_speed and short_trip)) then 'both'
        when codes[1] in ('DISTANCE_ABSURD', 'ZERO_DISTANCE_PAID') then 'distance'
        when codes[1] = 'SPEED_IMPLAUSIBLE' then if(short_trip, 'timestamps', 'distance')
        when codes[1] in ('TIMESTAMP_SUSPECT', 'LONG_DURATION') then 'timestamps'
        else 'fare'
    end as suspect_field,
    -- partition column: must stay last
    pickup_month
from coded
where cardinality(codes) > 0
