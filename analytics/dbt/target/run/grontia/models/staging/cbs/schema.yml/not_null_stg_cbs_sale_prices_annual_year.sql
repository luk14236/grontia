
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year
from "grontia_dev"."main_STAGING"."stg_cbs_sale_prices_annual"
where year is null



  
  
      
    ) dbt_internal_test