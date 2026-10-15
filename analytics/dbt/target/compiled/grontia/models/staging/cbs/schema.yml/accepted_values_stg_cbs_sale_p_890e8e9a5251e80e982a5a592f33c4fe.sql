
    
    

with all_values as (

    select
        period_type as value_field,
        count(*) as n_records

    from "grontia_dev"."main_STAGING"."stg_cbs_sale_price_index"
    group by period_type

)

select *
from all_values
where value_field not in (
    'quarter','year'
)


