select zone_id, borough, zone_name, service_zone
from {{ ref('stg_zones') }}
