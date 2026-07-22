
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select industry_code
from `stock-pulse-260629`.`stockpulse_staging`.`dim_industry`
where industry_code is null



  
  
      
    ) dbt_internal_test