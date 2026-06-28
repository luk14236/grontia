with source as (
    select * from read_parquet('s3://silver/knmi/daily_weather_all_stations/**/*.parquet')
),

renamed as (
    select
        trim(STN)                               as station_code,
        strptime(trim(YYYYMMDD), '%Y%m%d')::date as date,
        -- Temperaturas: KNMI usa décimos de grau Celsius
        try_cast(TG as double) / 10.0           as temp_avg_c,
        try_cast(TN as double) / 10.0           as temp_min_c,
        try_cast(TX as double) / 10.0           as temp_max_c,
        -- Precipitação: décimos de mm
        try_cast(RH as double) / 10.0           as precipitation_mm,
        -- Vento: décimos de m/s
        try_cast(FG as double) / 10.0           as wind_speed_ms,
        -- Campos extras comuns
        try_cast(SQ as double) / 10.0           as sunshine_hours,
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
