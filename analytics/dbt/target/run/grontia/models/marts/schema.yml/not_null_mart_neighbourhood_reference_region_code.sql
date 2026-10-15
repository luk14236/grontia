
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select region_code
from "grontia_dev"."main_MARTS"."mart_neighbourhood_reference"
where region_code is null



  
  
      
    ) dbt_internal_test