
    
    

select
    id as unique_field,
    count(*) as n_records

from "grontia_dev"."main_STAGING"."stg_pdok_bag_verblijfsobject"
where id is not null
group by id
having count(*) > 1


