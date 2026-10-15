-- CBS 85791NED: price index (2020=100), homes sold and average sale price of
-- existing homes per home type. National only; quarters (YYYYKWqq) and years (YYYYJJ00).
with source as (
    select * from read_parquet('s3://silver/cbs/sale_prices_by_home_type/**/*.parquet', hive_partitioning = true)
    qualify ingestion_date = max(ingestion_date) over ()
),

renamed as (
    select
        trim(TypeWoning)                                        as home_type_code,
        case trim(TypeWoning)
            when 'T001100' then 'total'
            when 'ZW10290' then 'single_family'
            when 'ZW25805' then 'terraced'
            when 'ZW25806' then 'corner'
            when 'ZW10300' then 'semi_detached'
            when 'ZW10320' then 'detached'
            when 'ZW25810' then 'apartment'
            when 'ZW25809' then 'unknown'
        end                                                     as home_type,
        trim(Perioden)                                          as period_code,
        try_cast(left(trim(Perioden), 4) as integer)            as year,
        case when trim(Perioden) like '%KW%' then 'quarter' else 'year' end as period_type,
        try_cast(PrijsindexVerkoopprijzen_1 as double)          as price_index_2020,
        try_cast(VerkochteWoningen_4 as integer)                as homes_sold,
        try_cast(GemiddeldeVerkoopprijs_7 as double)            as avg_sale_price_eur,
        ingestion_date::date                                    as ingestion_date,
        ingestion_timestamp
    from source
    where TypeWoning is not null
      and Perioden is not null
)

select * from renamed
