-- CBS 70675ned: average yearly rent increase of dwellings, national.
with source as (
    select * from read_parquet('s3://silver/cbs/rent_increase/**/*.parquet', hive_partitioning = true)
    qualify ingestion_date = max(ingestion_date) over ()
),

renamed as (
    select
        try_cast(left(trim(Perioden), 4) as integer)        as year,
        try_cast(Huurverhoging_1 as double)                 as rent_increase_pct,
        ingestion_date::date                                as ingestion_date,
        ingestion_timestamp
    from source
    where Perioden is not null
)

select * from renamed
