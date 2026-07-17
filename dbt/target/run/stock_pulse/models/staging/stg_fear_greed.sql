

  create or replace view `stock-pulse-260629`.`stockpulse_staging`.`stg_fear_greed`
  OPTIONS()
  as -- Fear & Greed 總經指標 staging 層
select
    dt as index_date,
    score as fear_greed_score,
    fear_greed_rating
from `stock-pulse-260629`.`stockpulse_staging`.`raw_fear_greed`;

