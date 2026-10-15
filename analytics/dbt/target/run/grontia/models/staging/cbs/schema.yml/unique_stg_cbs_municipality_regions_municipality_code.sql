
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    municipality_code as unique_field,
    count(*) as n_records

from "grontia_dev"."main_STAGING"."stg_cbs_municipality_regions"
where municipality_code is not null
group by municipality_code
having count(*) > 1



  
  
      
    ) dbt_internal_test