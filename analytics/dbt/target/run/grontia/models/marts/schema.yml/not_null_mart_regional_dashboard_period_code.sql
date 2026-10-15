
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period_code
from "grontia_dev"."main_MARTS"."mart_regional_dashboard"
where period_code is null



  
  
      
    ) dbt_internal_test