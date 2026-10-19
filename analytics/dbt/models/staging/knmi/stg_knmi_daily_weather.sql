with source as (
    select * from read_parquet('s3://silver/knmi/daily_weather_all_stations/**/*.parquet', hive_partitioning = true)
    -- each daily ingestion re-fetches the last 7 days: keep the latest value per station/day
    qualify row_number() over (partition by STN, YYYYMMDD order by ingestion_date desc) = 1
),

renamed as (
    select
        trim(STN)                               as station_code,
        strptime(trim(YYYYMMDD), '%Y%m%d')::date as date,
        -- Temperaturas: KNMI usa décimos de grau Celsius
        try_cast(TG as double) / 10.0           as temp_avg_c,
        try_cast(TN as double) / 10.0           as temp_min_c,
        try_cast(TX as double) / 10.0           as temp_max_c,
        -- Precipitação: décimos de mm; KNMI codes < 0.05 mm as -1
        case when try_cast(RH as integer) = -1 then 0.0
             else try_cast(RH as double) / 10.0 end as precipitation_mm,
        -- Vento: décimos de m/s
        try_cast(FG as double) / 10.0           as wind_speed_ms,
        -- Campos extras comuns
        -- KNMI codes < 0.05 h of sunshine as -1
        case when try_cast(SQ as integer) = -1 then 0.0
             else try_cast(SQ as double) / 10.0 end as sunshine_hours,
        try_cast(SP as double)                  as sunshine_pct,
        try_cast(Q  as double)                  as radiation_jcm2,
        try_cast(DR as double) / 10.0           as precip_duration_h,
        ingestion_date::date                    as ingestion_date,
        ingestion_timestamp
    from source
    where STN is not null
      and YYYYMMDD is not null
)

select * from renamed
