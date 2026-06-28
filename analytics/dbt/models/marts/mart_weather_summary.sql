{{
    config(
        materialized='table',
        tags=['mart', 'knmi', 'weather']
    )
}}

with weather as (
    select * from {{ ref('stg_knmi_daily_weather') }}
),

final as (
    select
        station_code,
        date,
        temp_avg_c,
        temp_min_c,
        temp_max_c,
        precipitation_mm,
        wind_speed_ms,
        sunshine_hours,
        sunshine_pct,
        radiation_jcm2,
        precip_duration_h,
        ingestion_date                      as last_ingested_date
    from weather
)

select * from final
