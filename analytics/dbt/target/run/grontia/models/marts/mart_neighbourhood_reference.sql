
    

    create  table
      "grontia_dev"."main_MARTS"."mart_neighbourhood_reference__dbt_tmp"
  
    
    as (
      

-- Reference price level for every municipality, district (wijk) and
-- neighbourhood (buurt). This is what an address evaluation compares an
-- asking price against.
--
-- 1. Municipality: average sale price of the last year CBS published
--    (83625NED), brought forward to the latest quarter with the regional price
--    index (85792NED). The index exists for the 4 largest municipalities and
--    for provinces; other municipalities use their province's index.
-- 2. District / neighbourhood: municipality estimate scaled by the ratio
--    between the area's average WOZ value and the municipality's (85984NED).
--    WOZ is the official tax valuation, so the ratio captures how much more or
--    less expensive the area is than its municipality.

with regions as (
    select *
    from "grontia_dev"."main_STAGING"."stg_cbs_neighbourhood_key_figures"
    where municipality_code is not null
),

municipality_woz as (
    select region_code as municipality_code, avg_woz_value_eur as municipality_avg_woz_eur
    from regions
    where region_type = 'Gemeente'
),

provinces as (
    select municipality_code, province_code, province_name
    from "grontia_dev"."main_STAGING"."stg_cbs_municipality_regions"
),

-- latest year with a published average sale price, per municipality
annual_price as (
    select
        region_code as municipality_code,
        year        as price_base_year,
        avg_sale_price_eur
    from "grontia_dev"."main_STAGING"."stg_cbs_sale_prices_annual"
    where region_code like 'GM%'
      and avg_sale_price_eur is not null
    qualify row_number() over (partition by region_code order by year desc) = 1
),

quarterly_index as (
    select region_code, year, quarter, period_code, price_index_2020
    from "grontia_dev"."main_STAGING"."stg_cbs_sale_price_index"
    where period_type = 'quarter'
      and price_index_2020 is not null
),

latest_quarter as (
    select region_code, period_code as index_latest_period, price_index_2020 as index_latest
    from quarterly_index
    qualify row_number() over (partition by region_code order by year desc, quarter desc) = 1
),

yearly_avg_index as (
    select region_code, year, avg(price_index_2020) as index_year_avg
    from quarterly_index
    group by region_code, year
),

-- big cities have their own index; everyone else uses the province index
municipality_index_region as (
    select
        a.*,
        p.province_code,
        p.province_name,
        case
            when a.municipality_code in (select region_code from latest_quarter)
                then a.municipality_code
            else p.province_code
        end as index_region_code
    from annual_price a
    left join provinces p using (municipality_code)
),

municipality_estimate as (
    select
        m.municipality_code,
        m.province_code,
        m.province_name,
        m.price_base_year,
        m.avg_sale_price_eur                                    as avg_sale_price_base_year_eur,
        m.index_region_code,
        lq.index_latest_period,
        round(lq.index_latest / ya.index_year_avg - 1, 4)       as price_change_since_base_year,
        round(m.avg_sale_price_eur * lq.index_latest / ya.index_year_avg, -3)
                                                                as est_current_avg_sale_price_eur
    from municipality_index_region m
    left join latest_quarter   lq on lq.region_code = m.index_region_code
    left join yearly_avg_index ya
        on ya.region_code = m.index_region_code
       and ya.year = m.price_base_year
),

final as (
    select
        r.region_code,
        r.region_type,
        r.municipality_code,
        r.municipality_name,
        m.province_code,
        m.province_name,
        r.population,
        r.households,
        r.dwellings,
        r.avg_income_per_resident_eur,
        r.avg_woz_value_eur,
        mw.municipality_avg_woz_eur,
        round(r.avg_woz_value_eur / nullif(mw.municipality_avg_woz_eur, 0), 3)
                                                    as woz_ratio_to_municipality,
        m.price_base_year,
        m.avg_sale_price_base_year_eur              as municipality_avg_sale_price_base_year_eur,
        m.index_region_code,
        m.index_latest_period,
        m.price_change_since_base_year,
        m.est_current_avg_sale_price_eur            as municipality_est_current_avg_sale_price_eur,
        round(
            m.est_current_avg_sale_price_eur
            * coalesce(r.avg_woz_value_eur / nullif(mw.municipality_avg_woz_eur, 0), 1),
            -3
        )                                           as est_current_avg_sale_price_eur,
        r.ingestion_date                            as last_ingested_date
    from regions r
    left join municipality_woz      mw using (municipality_code)
    left join municipality_estimate m  using (municipality_code)
)

select * from final
    );
    
  