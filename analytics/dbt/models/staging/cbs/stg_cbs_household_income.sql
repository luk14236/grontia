-- CBS 86161NED, filtered to all private households at ingestion.
with source as (
    select * from read_parquet('s3://silver/cbs/household_income/**/*.parquet', hive_partitioning = true)
    qualify ingestion_date = max(ingestion_date) over ()
),

renamed as (
    select
        trim(RegioS)                                                as region_code,
        trim(Perioden)                                              as period_code,
        try_cast(ParticuliereHuishoudens_1 as double) * 1000        as private_households,
        -- CBS publishes income in thousands of euros
        try_cast(GemiddeldBesteedbaarInkomen_5 as double) * 1000    as avg_disposable_income_eur,
        try_cast(MediaanBesteedbaarInkomen_6 as double) * 1000      as median_disposable_income_eur,
        ingestion_date::date                                        as ingestion_date,
        ingestion_timestamp
    from source
    where RegioS is not null
      and Perioden is not null
)

select * from renamed
