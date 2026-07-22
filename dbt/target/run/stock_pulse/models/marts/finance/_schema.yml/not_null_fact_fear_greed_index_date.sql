
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select index_date
from `stock-pulse-260629`.`stockpulse_staging`.`fact_fear_greed`
where index_date is null



  
  
      
    ) dbt_internal_test