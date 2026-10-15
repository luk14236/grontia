
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from "grontia_dev"."main_STAGING"."stg_pdok_bag_pand"
where id is null



  
  
      
    ) dbt_internal_test