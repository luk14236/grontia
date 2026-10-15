
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select province_code
from "grontia_dev"."main_STAGING"."stg_cbs_municipality_regions"
where province_code is null



  
  
      
    ) dbt_internal_test