-- CBS 85792NED: sale price index (2020=100) of existing homes.
-- Regions: NL, landsdelen, provinces and the 4 largest municipalities.
-- Periods are quarters (YYYYKWqq) plus yearly totals (YYYYJJ00).
with source as (
    select * from read_parquet('s3://silver/cbs/sale_price_index_quarterly/**/*.parquet', hive_partitioning = true)
    qualify ingestion_date = max(ingestion_date) over ()
),

renamed as (
    select
        trim(RegioS)                                            as region_code,
        trim(Perioden)                                          as period_code,
        try_cast(left(trim(Perioden), 4) as integer)            as year,
        case when trim(Perioden) like '%KW%'
             then try_cast(right(trim(Perioden), 2) as integer)
        end                                                     as quarter,
        case when trim(Perioden) like '%KW%' then 'quarter' else 'year' end as period_type,
        try_cast(PrijsindexVerkoopprijzen_1 as double)          as price_index_2020,
        try_cast(OntwikkelingTOVEenJaarEerder_3 as double)      as price_index_yoy_pct,
        try_cast(VerkochteWoningen_4 as integer)                as homes_sold,
        try_cast(GemiddeldeVerkoopprijs_7 as double)            as avg_sale_price_eur,
        ingestion_date::date                                    as ingestion_date,
        ingestion_timestamp
    from source
    where RegioS is not null
      and Perioden is not null
)

select * from renamed