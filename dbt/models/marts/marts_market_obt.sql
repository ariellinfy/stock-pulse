-- dbt/models/marts/marts_market_obt.sql

{{
  config(
    materialized='table'
  )
}}

select
    f.trade_date,
    f.stock_id,
    s.company_name,
    s.stock_name,
    f.market,
    s.industry_code,
    f.open_price,
    f.high_price,
    f.low_price,
    f.close_price,
    f.trade_volume,
    f.trade_value,
    f.change_amount,
    f.ma5,
    f.ma20,
    f.rsi14,
    ip.weighted_avg_price as industry_weighted_price,
    fg.fear_greed_score,
    fg.fear_greed_rating
from {{ ref('fact_stock_daily') }} f
left join {{ ref('dim_stock') }} s
    on f.stock_id = s.stock_id
left join {{ ref('fact_industry_price') }} ip
    on f.trade_date = ip.trade_date
    and s.industry_code = ip.industry_code
    and f.market = ip.market
left join {{ ref('fact_fear_greed') }} fg
    on f.trade_date = fg.index_date