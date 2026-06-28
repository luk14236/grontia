with source as (
    select * from read_parquet('s3://silver/pdok/bag_verblijfsobject/**/*.parquet')
),

renamed as (
    select
        id                                      as id,
        geometry                                as geometry,
        identificatie                           as identificatie,
        status                                  as status,
        gebruiksdoel                            as gebruiksdoel,
        try_cast(oppervlakte as double)         as oppervlakte_m2,
        ingestion_date::date                    as ingestion_date,
        ingestion_timestamp
    from source
    where id is not null
)

select * from renamed
