-- Typical traffic per NDW measurement site: average speed and flow by day type
-- (weekday / weekend) and local hour, over the last four weeks of hourly rollups.
with hourly as (
    select
        *,
        hour_start at time zone 'Europe/Amsterdam' as local_hour_start
    from {{ ref('stg_ndw_traffic_hourly') }}
)

select
    site_id,
    case when isodow(local_hour_start) in (6, 7) then 'weekend' else 'weekday' end as day_type,
    hour(local_hour_start)              as hour_of_day,
    round(avg(avg_speed_kmh), 1)        as avg_speed_kmh,
    round(avg(avg_flow_per_hour), 0)    as avg_flow_per_hour,
    count(*)                            as n_hours,
    min(hour_start)                     as first_hour,
    max(hour_start)                     as last_hour
from hourly
group by all
