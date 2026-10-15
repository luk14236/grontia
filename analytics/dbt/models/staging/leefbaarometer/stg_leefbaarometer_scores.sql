-- Leefbaarometer 3.0: livability score per neighbourhood, district and
-- municipality, every measurement year since 2002 (biennial since 2012).
--   lbm   overall score (higher = more livable)
--   afw   deviation from the national average
--   fys/onv/soc/vrz/won  contribution of physical environment, safety
--   (onveiligheid: higher = safer), social cohesion, amenities and housing stock.
--   Dimensions are only published for 2014 and from 2018 on.
with source as (
    select * from read_parquet('s3://silver/leefbaarometer/scores/**/*.parquet', hive_partitioning = true)
    qualify ingestion_date = max(ingestion_date) over ()
)

select
    region_level,
    trim(region_code)                   as region_code,
    region_name,
    try_cast(jaar as integer)           as year,
    try_cast(lbm as double)             as livability_score,
    try_cast(afw as double)             as deviation_from_national,
    try_cast(fys as double)             as physical_environment,
    try_cast(onv as double)             as safety,
    try_cast(soc as double)             as social_cohesion,
    try_cast(vrz as double)             as amenities,
    try_cast(won as double)             as housing_stock,
    ingestion_date::date                as ingestion_date,
    ingestion_timestamp
from source
where region_code is not null
