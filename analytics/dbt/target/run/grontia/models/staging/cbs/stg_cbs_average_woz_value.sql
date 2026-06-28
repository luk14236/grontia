
  
  create view "grontia_dev"."main_STAGING"."stg_cbs_average_woz_value__dbt_tmp" as (
    with source as (
    select * from read_parquet('s3://silver/cbs/average_woz_value/**/*.parquet')
),

renamed as (
    select
        RegioS                                          as region_code,
        RegioNaam                                       as region_name,
        Perioden                                        as period_code,
        try_cast(GemiddeldeWOZWaarde_1 as double)       as avg_woz_value_eur,
        ingestion_date::date                            as ingestion_date,
        ingestion_timestamp
    from source
    where RegioS is not null
      and Perioden is not null
)

select * from renamed
  );
