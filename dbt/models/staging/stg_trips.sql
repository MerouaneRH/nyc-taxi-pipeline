-- Typed/renamed yellow trips. Drops impossible rows and dedups on trip_id.
with src as (
    select * from {{ source('raw', 'yellow_trips') }}
    -- partition predicate => projection prunes S3 scan (cost control)
    where year between {{ var('min_year') }} and {{ var('max_year') }}
),

typed as (
    select
        to_hex(md5(to_utf8(concat_ws('|',
            cast(vendorid as varchar),
            cast(tpep_pickup_datetime as varchar),
            cast(tpep_dropoff_datetime as varchar),
            cast(pulocationid as varchar),
            cast(dolocationid as varchar),
            cast(total_amount as varchar))))) as trip_id,
        cast(vendorid as integer) as vendor_id,
        tpep_pickup_datetime as pickup_at,
        tpep_dropoff_datetime as dropoff_at,
        cast(passenger_count as integer) as passenger_count,
        trip_distance as trip_distance_miles,
        cast(ratecodeid as integer) as rate_code_id,
        store_and_fwd_flag,
        pulocationid as pickup_zone_id,
        dolocationid as dropoff_zone_id,
        cast(payment_type as integer) as payment_type,
        fare_amount, extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, congestion_surcharge,
        airport_fee,
        total_amount,
        year as src_year,
        month as src_month
    from src
),

valid as (
    select * from typed
    where pickup_at is not null
      and dropoff_at >= pickup_at
      and date_diff('hour', pickup_at, dropoff_at) < 24
      and trip_distance_miles >= 0
      and total_amount >= 0
      -- pickup must fall in the file's own month (files hold stray dates)
      and year(pickup_at) = src_year
      and month(pickup_at) = src_month
),

dedup as (
    select *, row_number() over (partition by trip_id order by pickup_at) as rn
    from valid
)

select
    trip_id, vendor_id, pickup_at, dropoff_at, passenger_count, trip_distance_miles,
    rate_code_id, store_and_fwd_flag, pickup_zone_id, dropoff_zone_id, payment_type,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
    congestion_surcharge, airport_fee, total_amount, src_year, src_month
from dedup
where rn = 1
