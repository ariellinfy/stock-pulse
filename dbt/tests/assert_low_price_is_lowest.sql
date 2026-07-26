-- dbt/tests/assert_low_price_is_lowest.sql
select *
from {{ ref('fact_stock_daily') }}
where low_price is not null
  and (market = 'TWSE' or (market = 'TPEx' and trade_value is not null))
  and (
    low_price > open_price
    or low_price > high_price
    or low_price > close_price
  )
