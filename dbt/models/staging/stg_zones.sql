select
    cast(locationid as integer) as zone_id,
    borough,
    zone as zone_name,
    service_zone
from {{ source('raw', 'taxi_zone_lookup') }}
where locationid is not null and locationid <> ''
