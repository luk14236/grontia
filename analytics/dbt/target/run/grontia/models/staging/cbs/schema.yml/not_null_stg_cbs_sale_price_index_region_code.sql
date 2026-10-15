
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select region_code
from "grontia_dev"."main_STAGING"."stg_cbs_sale_price_index"
where region_code is null



  
  
      
    ) dbt_internal_test