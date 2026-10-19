-- Hourly rollup per NDW measurement site, written by DAG 60 (60_ndw_traffic_live).
-- Ephemeral: it is only an input to mart_traffic_site_profile, and as a table
-- on MotherDuck it would copy ~500k rows per day into the cloud.
{{ config(materialized='ephemeral') }}

with source as (
    select * from {{ read_silver('ndw/traffic_hourly') }}
    -- the profile looks at four weeks; DAG 60 keeps 35 days of hourly files
    where "date" >= current_date - 28
)

select
    site_id,
    hour_start,
    avg_speed_kmh,
    avg_flow_per_hour,
    n_measurements
from source
