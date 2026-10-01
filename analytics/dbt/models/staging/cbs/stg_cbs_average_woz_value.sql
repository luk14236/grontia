-- CBS 85036NED, filtered to Eigendom = Totaal (all ownership types) at ingestion.
with source as (
    select * from read_parquet('s3://silver/cbs/average_woz_value/**/*.parquet', hive_partitioning = true)
    qualify ingestion_date = max(ingestion_date) over ()
),

renamed as (
    select
        trim(RegioS)                                            as region_code,
        trim(Perioden)                                          as period_code,
        -- CBS publishes WOZ values in thousands of euros
        try_cast(GemiddeldeWOZWaardeVanWoningen_1 as double) * 1000 as avg_woz_value_eur,
        ingestion_date::date                                    as ingestion_date,
        ingestion_timestamp
    from source
    where RegioS is not null
      and Perioden is not null
)

select * from renamed
