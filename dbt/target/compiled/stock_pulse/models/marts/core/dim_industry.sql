-- dbt/models/marts/core/dim_industry.sql



select distinct
    u.industry_code,
    m.industry_name
from `stock-pulse-260629`.`stockpulse_staging`.`stg_stock_universe` u
left join `stock-pulse-260629`.`stockpulse_staging`.`industry_code_mapping` m
    on u.industry_code = m.industry_code
where u.industry_code is not null