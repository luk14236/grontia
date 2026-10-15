
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from "grontia_dev"."main_STAGING"."stg_knmi_daily_weather"
where date is null



  
  
      
    ) dbt_internal_test