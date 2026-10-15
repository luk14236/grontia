
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_woz_value_eur
from "grontia_dev"."main_STAGING"."stg_cbs_average_woz_value"
where avg_woz_value_eur is null



  
  
      
    ) dbt_internal_test