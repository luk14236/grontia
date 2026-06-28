with source as (
    select * from read_parquet('s3://silver/ndw/current_traffic_flow/**/*.parquet')
),

renamed as (
    select
        id                                              as id,
        element_type                                    as element_type,
        measurementSiteReference                        as measurement_site_ref,
        period                                          as period,
        try_cast(vehicleFlowRate as integer)            as vehicle_flow_rate,
        try_cast(averageVehicleSpeed as double)         as avg_vehicle_speed_kmh,
        ingestion_date::date                            as ingestion_date,
        ingestion_timestamp
    from source
    where id is not null
)

select * from renamed
