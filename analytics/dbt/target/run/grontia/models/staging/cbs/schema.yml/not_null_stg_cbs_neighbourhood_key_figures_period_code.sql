
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period_code
from "grontia_dev"."main_STAGING"."stg_cbs_neighbourhood_key_figures"
where period_code is null



  
  
      
    ) dbt_internal_test