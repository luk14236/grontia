{{
    config(
        materialized='table',
        tags=['mart', 'cbs', 'dashboard']
    )
}}

with neighbourhood as (
    select * from {{ ref('stg_cbs_neighbourhood_key_figures') }}
),

woz as (
    select
        region_code,
        period_code,
        avg_woz_value_eur
    from {{ ref('stg_cbs_average_woz_value') }}
),

income as (
    select
        region_code,
        period_code,
        avg_household_income_eur,
        median_household_income_eur
    from {{ ref('stg_cbs_household_income') }}
),

final as (
    select
        n.region_code,
        n.region_name,
        n.region_type,
        n.period_code,
        n.population,
        n.households,
        n.avg_income_per_resident_eur,
        w.avg_woz_value_eur,
        i.avg_household_income_eur,
        i.median_household_income_eur,
        n.ingestion_date                as last_ingested_date
    from neighbourhood n
    left join woz    w using (region_code, period_code)
    left join income i using (region_code, period_code)
)

select * from final
