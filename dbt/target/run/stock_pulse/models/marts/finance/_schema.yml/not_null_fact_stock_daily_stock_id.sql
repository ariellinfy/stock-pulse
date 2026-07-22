
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stock_id
from `stock-pulse-260629`.`stockpulse_staging`.`fact_stock_daily`
where stock_id is null



  
  
      
    ) dbt_internal_test