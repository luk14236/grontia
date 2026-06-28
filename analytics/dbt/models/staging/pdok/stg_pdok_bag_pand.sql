with source as (
    select * from read_parquet('s3://silver/pdok/bag_pand/**/*.parquet')
),

renamed as (
    select
        id                                      as id,
        geometry                                as geometry,
        identificatie                           as identificatie,
        status                                  as status,
        try_cast(bouwjaar as integer)           as bouwjaar,
        ingestion_date::date                    as ingestion_date,
        ingestion_timestamp
    from source
    where id is not null
)

select * from renamed
