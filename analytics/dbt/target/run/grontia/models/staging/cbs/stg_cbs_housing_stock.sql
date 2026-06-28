
  
  create view "grontia_dev"."main_STAGING"."stg_cbs_housing_stock__dbt_tmp" as (
    with source as (
    select * from read_parquet('s3://silver/cbs/housing_stock/**/*.parquet')
),

renamed as (
    select
        RegioS                                  as region_code,
        RegioNaam                               as region_name,
        Perioden                                as period_code,
        TypeWoning_3                            as dwelling_type,
        try_cast(AantalWoningen_1 as integer)   as number_of_dwellings,
        ingestion_date::date                    as ingestion_date,
        ingestion_timestamp
    from source
    where RegioS is not null
      and Perioden is not null
)

select * from renamed
  );
