-- dbt/tests/assert_high_price_is_highest.sql
select *
from {{ ref('fact_stock_daily') }}
where high_price is not null
  and (market = 'TWSE' or (market = 'TPEx' and trade_value is not null))
  and (
    high_price < open_price
    or high_price < low_price
    or high_price < close_price
  )
