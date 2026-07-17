
  
    

    create or replace table `stock-pulse-260629`.`stockpulse_staging`.`fact_industry_price`
      
    
    

    
    OPTIONS()
    as (
      -- dbt/models/marts/finance/fact_industry_price.sql



select
    trade_date,
    industry_code,
    market,
    total_weight_basis,
    weighted_avg_price,
    stock_count,
    weighting_method
from `stock-pulse-260629`.`stockpulse_staging`.`int_industry_daily_price`
    );
  