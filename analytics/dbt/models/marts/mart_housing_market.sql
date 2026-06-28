{{
    config(
        materialized='table',
        tags=['mart', 'cbs', 'housing']
    )
}}

with housing as (
    select * from {{ ref('stg_cbs_housing_stock') }}
),

woz as (
    select
        region_code,
        period_code,
        avg_woz_value_eur
    from {{ ref('stg_cbs_average_woz_value') }}
),

final as (
    select
        h.region_code,
        h.region_name,
        h.period_code,
        h.dwelling_type,
        h.number_of_dwellings,
        w.avg_woz_value_eur,
        h.ingestion_date                    as last_ingested_date
    from housing h
    left join woz w using (region_code, period_code)
)

select * from final
