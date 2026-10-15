
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select station_code
from "grontia_dev"."main_STAGING"."stg_knmi_daily_weather"
where station_code is null



  
  
      
    ) dbt_internal_test