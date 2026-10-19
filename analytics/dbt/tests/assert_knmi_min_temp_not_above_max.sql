-- A day's minimum temperature can never exceed its maximum.
select station_code, date, temp_min_c, temp_max_c
from {{ ref('stg_knmi_daily_weather') }}
where temp_min_c > temp_max_c
