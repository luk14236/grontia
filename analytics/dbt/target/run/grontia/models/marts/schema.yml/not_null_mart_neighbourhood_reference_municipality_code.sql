
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select municipality_code
from "grontia_dev"."main_MARTS"."mart_neighbourhood_reference"
where municipality_code is null



  
  
      
    ) dbt_internal_test