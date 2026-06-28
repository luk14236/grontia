with source as (
    select * from read_parquet('s3://silver/cbs/neighbourhood_key_figures/**/*.parquet')
),

renamed as (
    select
        RegioS                                      as region_code,
        RegioNaam                                   as region_name,
        SoortRegio_2                                as region_type,
        Perioden                                    as period_code,
        try_cast(Inwoners_5 as integer)             as population,
        try_cast(AantalHuishoudens_10 as integer)   as households,
        try_cast(GemiddeldInkomenPerInwoner_25 as double) as avg_income_per_resident_eur,
        ingestion_date::date                        as ingestion_date,
        ingestion_timestamp
    from source
    where RegioS is not null
      and Perioden is not null
)

select * from renamed