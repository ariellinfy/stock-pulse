-- dbt/models/marts/finance/fact_fear_greed.sql



select
    index_date,
    fear_greed_score,
    fear_greed_rating
from `stock-pulse-260629`.`stockpulse_staging`.`stg_fear_greed`