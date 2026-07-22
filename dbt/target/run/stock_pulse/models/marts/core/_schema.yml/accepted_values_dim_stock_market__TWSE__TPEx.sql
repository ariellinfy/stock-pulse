
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        market as value_field,
        count(*) as n_records

    from `stock-pulse-260629`.`stockpulse_staging`.`dim_stock`
    group by market

)

select *
from all_values
where value_field not in (
    'TWSE','TPEx'
)



  
  
      
    ) dbt_internal_test