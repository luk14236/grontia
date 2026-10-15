-- CBS 83625NED: average sale price of existing owner-occupied homes, per year.
with source as (
    select * from read_parquet('s3://silver/cbs/sale_prices_annual/**/*.parquet', hive_partitioning = true)
    qualify ingestion_date = max(ingestion_date) over ()
),

renamed as (
    select
        trim(RegioS)                                        as region_code,
        trim(Perioden)                                      as period_code,
        try_cast(left(trim(Perioden), 4) as integer)        as year,
        try_cast(GemiddeldeVerkoopprijs_1 as double)        as avg_sale_price_eur,
        ingestion_date::date                                as ingestion_date,
        ingestion_timestamp
    from source
    where RegioS is not null
      and Perioden is not null
)

select * from renamed
