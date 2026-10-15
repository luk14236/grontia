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
        -- Housing mix (percentages of the dwelling stock)
        try_cast(PercentageMeergezinswoning_45 as double)   as pct_apartments,
        try_cast(Koopwoningen_47 as double)                 as pct_owner_occupied,
        try_cast(HuurwoningenTotaal_48 as double)           as pct_rental,
        try_cast(InBezitWoningcorporatie_49 as double)      as pct_social_housing,
        try_cast(BouwjaarAfgelopenTienJaar_52 as double)    as pct_built_last_10_years,
        try_cast(OnbewoondeWoningen_46 as double)           as pct_vacant,
        -- Distances in km to the nearest amenity
        try_cast(AfstandTotHuisartsenpraktijk_110 as double) as km_to_gp,
        try_cast(AfstandTotGroteSupermarkt_111 as double)   as km_to_supermarket,
        try_cast(AfstandTotKinderdagverblijf_112 as double) as km_to_daycare,
        try_cast(AfstandTotSchool_113 as double)            as km_to_school,
        -- 1 = very strongly urban ... 5 = not urban
        try_cast(MateVanStedelijkheid_120 as integer)       as urbanity_class,
        ingestion_date::date                                as ingestion_date,
        ingestion_timestamp
    from source
    where WijkenEnBuurten is not null
)

select * from renamed
