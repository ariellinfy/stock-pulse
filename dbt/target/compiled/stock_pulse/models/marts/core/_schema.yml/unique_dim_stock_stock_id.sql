
    
    

with dbt_test__target as (

  select stock_id as unique_field
  from `stock-pulse-260629`.`stockpulse_staging`.`dim_stock`
  where stock_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


