
    

    create  table
      "grontia_dev"."main_MARTS"."mart_housing_market__dbt_tmp"
  
    
    as (
      

with housing as (
    select * from "grontia_dev"."main_STAGING"."stg_cbs_housing_stock"
),

woz as (
    select
        region_code,
        period_code,
        avg_woz_value_eur
    from "grontia_dev"."main_STAGING"."stg_cbs_average_woz_value"
),

-- 85984NED names every municipality (GM codes); used to label RegioS codes
municipalities as (
    select region_code, municipality_name
    from "grontia_dev"."main_STAGING"."stg_cbs_neighbourhood_key_figures"
    where region_type = 'Gemeente'
),

final as (
    select
        h.region_code,
        m.municipality_name,
        h.period_code,
        h.total_dwellings,
        h.owner_occupied_dwellings,
        h.rental_dwellings,
        h.social_housing_dwellings,
        round(h.owner_occupied_dwellings / nullif(h.total_dwellings, 0), 3) as owner_occupied_share,
        w.avg_woz_value_eur,
        h.ingestion_date                    as last_ingested_date
    from housing h
    left join woz w using (region_code, period_code)
    left join municipalities m using (region_code)
)

select * from final
    );
    
  