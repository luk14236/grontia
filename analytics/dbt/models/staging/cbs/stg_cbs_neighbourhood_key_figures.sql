-- CBS 85984NED "Kerncijfers wijken en buurten 2024": one row per
-- country / municipality (GM) / district (WK) / neighbourhood (BU).
-- The table covers a single year, so period_code is constant.
with source as (
    select * from read_parquet('s3://silver/cbs/neighbourhood_key_figures/**/*.parquet', hive_partitioning = true)
    qualify ingestion_date = max(ingestion_date) over ()
),

renamed as (
    select
        trim(WijkenEnBuurten)                               as region_code,
        trim(Gemeentenaam_1)                                as municipality_name,
        trim(SoortRegio_2)                                  as region_type,
        -- WK/BU codes embed the 4-digit municipality number (WK036300, BU03630000)
        case
            when trim(WijkenEnBuurten) like 'GM%' then trim(WijkenEnBuurten)
            when trim(WijkenEnBuurten) like 'WK%' or trim(WijkenEnBuurten) like 'BU%'
                then 'GM' || substr(trim(WijkenEnBuurten), 3, 4)
        end                                                 as municipality_code,
        '2024JJ00'                                          as period_code,
        try_cast(AantalInwoners_5 as integer)               as population,
        try_cast(HuishoudensTotaal_29 as integer)           as households,
        -- CBS publishes income in thousands of euros
        try_cast(GemiddeldInkomenPerInwoner_78 as double) * 1000 as avg_income_per_resident_eur,
        try_cast(Woningvoorraad_35 as integer)              as dwellings,
        -- CBS publishes WOZ values in thousands of euros
        try_cast(GemiddeldeWOZWaardeVanWoningen_39 as double) * 1000 as avg_woz_value_eur,
        ingestion_date::date                                as ingestion_date,
        ingestion_timestamp
    from source
    where WijkenEnBuurten is not null
)

select * from renamed
