-- 股票清單 staging 層:輕量整理產業分類清單,作為下游 dim_stock 的來源
select
    stock_id,
    company_name,
    stock_name,
    industry_code,
    listing_date,
    market
from `stock-pulse-260629`.`stockpulse_staging`.`raw_industry_list`