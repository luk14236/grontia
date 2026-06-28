with source as (
    select * from read_parquet('s3://silver/cbs/household_income/**/*.parquet')
),

renamed as (
    select
        RegioS                                                  as region_code,
        RegioNaam                                               as region_name,
        Perioden                                                as period_code,
        try_cast(GemiddeldInkomenHuishouden_6 as double)        as avg_household_income_eur,
        try_cast(MediaanInkomenHuishouden_7 as double)          as median_household_income_eur,
        ingestion_date::date                                    as ingestion_date,
        ingestion_timestamp
    from source
    where RegioS is not null
      and Perioden is not null
)

select * from renamed
