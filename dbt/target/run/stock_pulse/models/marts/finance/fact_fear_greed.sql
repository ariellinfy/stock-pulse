
  
    

    create or replace table `stock-pulse-260629`.`stockpulse_staging`.`fact_fear_greed`
      
    
    

    
    OPTIONS()
    as (
      -- dbt/models/marts/finance/fact_fear_greed.sql



select
    index_date,
    fear_greed_score,
    fear_greed_rating
from `stock-pulse-260629`.`stockpulse_staging`.`stg_fear_greed`
    );
  