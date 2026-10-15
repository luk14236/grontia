
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    region_code as unique_field,
    count(*) as n_records

from "grontia_dev"."main_MARTS"."mart_neighbourhood_reference"
where region_code is not null
group by region_code
having count(*) > 1



  
  
      
    ) dbt_internal_test