
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test