
  
    

    create or replace table `stock-pulse-260629`.`stockpulse_staging`.`dim_stock`
      
    
    

    
    OPTIONS()
    as (
      -- dbt/models/marts/core/dim_stock.sql



select
    stock_id,
    company_name,
    stock_name,
    industry_code,
    listing_date,
    market
from `stock-pulse-260629`.`stockpulse_staging`.`stg_stock_universe`
    );
  