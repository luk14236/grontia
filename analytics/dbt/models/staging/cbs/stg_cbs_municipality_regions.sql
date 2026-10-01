-- CBS 85755NED "Gebieden in Nederland 2024": one row per municipality with the
-- higher-level regions it belongs to. Same municipal borders as 85984NED.
with source as (
    select * from read_parquet('s3://silver/cbs/municipality_regions/**/*.parquet', hive_partitioning = true)
    qualify ingestion_date = max(ingestion_date) over ()
),

renamed as (
    select
        trim(Code_1)                as municipality_code,
        trim(Naam_2)                as municipality_name,
        trim(Code_28)               as province_code,
        trim(Naam_29)               as province_name,
        ingestion_date::date        as ingestion_date,
        ingestion_timestamp
    from source
    where Code_1 is not null
)

select * from renamed
