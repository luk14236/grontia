-- CBS 82900NED, filtered to StatusVanBewoning = Totaal (occupied + vacant) at ingestion.
with source as (
    select * from read_parquet('s3://silver/cbs/housing_stock/**/*.parquet', hive_partitioning = true)
    qualify ingestion_date = max(ingestion_date) over ()
),

renamed as (
    select
        trim(RegioS)                                        as region_code,
        trim(Perioden)                                      as period_code,
        try_cast(TotaleWoningvoorraad_1 as integer)         as total_dwellings,
        try_cast(Koopwoningen_2 as integer)                 as owner_occupied_dwellings,
        try_cast(TotaalHuurwoningen_3 as integer)           as rental_dwellings,
        try_cast(EigendomWoningcorporatie_4 as integer)     as social_housing_dwellings,
        ingestion_date::date                                as ingestion_date,
        ingestion_timestamp
    from source
    where RegioS is not null
      and Perioden is not null
)

select * from renamed
