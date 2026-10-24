-- Passes when empty: sum(trips) per month in mart_od_pairs equals count(*) of fct_trips.
with m as (
    select pickup_month, sum(trips) as n from {{ ref('mart_od_pairs') }} group by 1
),
f as (
    select pickup_month, count(*) as n from {{ ref('fct_trips') }} group by 1
)
select coalesce(m.pickup_month, f.pickup_month) as pickup_month, m.n as mart_trips, f.n as fct_trips
from m full outer join f on m.pickup_month = f.pickup_month
where m.n is distinct from f.n
