-- 個股每日行情 staging 層
select
    dt as trade_date,
    stock_id,
    stock_name,
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
    pe_ratio,
    issued_shares
from `stock-pulse-260629`.`stockpulse_staging`.`raw_stock_daily`