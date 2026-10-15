
  
  create view "grontia_dev"."main_STAGING"."stg_ndw_traffic_flow__dbt_tmp" as (
    with source as (
    select * from read_parquet('s3://silver/ndw/current_traffic_flow/**/*.parquet', hive_partitioning = true)
    qualify row_number() over (partition by id, period order by ingestion_date desc) = 1
),

renamed as (
    select
        id                                              as id,
        element_type                                    as element_type,
        measurementSiteReference                        as measurement_site_ref,
        try_cast(period as timestamptz)                 as measured_at,
        try_cast(vehicleFlowRate as integer)            as vehicle_flow_rate_per_hour,
        try_cast(averageVehicleSpeed as double)         as avg_vehicle_speed_kmh,
        ingestion_date::date                            as ingestion_date,
        ingestion_timestamp
    from source
    where id is not null
)

select * from renamed
  );
