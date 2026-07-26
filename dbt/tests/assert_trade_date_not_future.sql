-- dbt/tests/assert_trade_date_not_future.sql
select *
from {{ ref('fact_stock_daily') }}
where trade_date > current_date('Asia/Taipei')
  and trade_date >= date_sub(current_date('Asia/Taipei'), interval 7 day)
  