-- dbt/models/marts/finance/fact_stock_daily.sql



with base as (
    select
        trade_date, stock_id, market, open_price, high_price, low_price,
        close_price, average_price, trade_volume, trade_value,
        transaction_count, change_amount
    from `stock-pulse-260629`.`stockpulse_staging`.`stg_twse_tpex_daily`
    
    -- 只處理最近的資料,但 window function 需要往回看歷史算 MA/RSI,
    -- 所以要往前多抓一段緩衝(至少 20 天,對應 MA20 需要的窗口)
    where trade_date >= (select date_sub(max(trade_date), interval 30 day) from `stock-pulse-260629`.`stockpulse_staging`.`fact_stock_daily`)
    
),
-- ...(with_prev_close、with_price_change、with_indicators 邏輯不變)

with_prev_close as (
    select
        *,
        lag(close_price) over (partition by stock_id order by trade_date) as prev_close_price
    from base
),

with_price_change as (
    select
        *,
        case when close_price > prev_close_price then close_price - prev_close_price else 0 end as gain,
        case when close_price < prev_close_price then prev_close_price - close_price else 0 end as loss
    from with_prev_close
),

with_indicators as (
    select
        *,
        -- MA5、MA20:過去 N 天(含當天)收盤價的簡單移動平均
        avg(close_price) over (
            partition by stock_id order by trade_date
            rows between 4 preceding and current row
        ) as ma5,
        avg(close_price) over (
            partition by stock_id order by trade_date
            rows between 19 preceding and current row
        ) as ma20,
        -- RSI14: 簡化版(用簡單移動平均計算平均漲跌幅,非標準平滑版本)
        avg(gain) over (
            partition by stock_id order by trade_date
            rows between 13 preceding and current row
        ) as avg_gain_14,
        avg(loss) over (
            partition by stock_id order by trade_date
            rows between 13 preceding and current row
        ) as avg_loss_14
    from with_price_change
)

select
    trade_date,
    stock_id,
    market,
    open_price,
    high_price,
    low_price,
    close_price,
    average_price,
    trade_volume,
    trade_value,
    transaction_count,
    change_amount,
    ma5,
    ma20,
    case
        when avg_loss_14 = 0 then 100
        else 100 - (100 / (1 + avg_gain_14 / nullif(avg_loss_14, 0)))
    end as rsi14
from with_indicators