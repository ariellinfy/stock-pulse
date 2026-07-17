
  
    

    create or replace table `stock-pulse-260629`.`stockpulse_staging`.`int_industry_daily_price`
      
    
    

    
    OPTIONS()
    as (
      -- dbt/models/intermediate/int_industry_daily_price.sql



with stock_with_industry as (
    select
        s.trade_date,
        s.stock_id,
        s.close_price,
        s.trade_value,
        s.trade_volume,
        u.industry_code,
        s.market,  -- 改用行情資料本身的市場別,反映交易當下的真實市場,不受清單「現在」快照影響
        coalesce(s.trade_value, s.trade_volume) as weight_basis,
        case when s.trade_value is not null then 'trade_value' else 'trade_volume' end as weighting_method
    from `stock-pulse-260629`.`stockpulse_staging`.`stg_twse_tpex_daily` s
    inner join `stock-pulse-260629`.`stockpulse_staging`.`stg_stock_universe` u
        on s.stock_id = u.stock_id
    where coalesce(s.trade_value, s.trade_volume) > 0
      and s.close_price is not null
)

select
    trade_date,
    industry_code,
    market,
    sum(weight_basis) as total_weight_basis,
    sum(close_price * weight_basis) / nullif(sum(weight_basis), 0) as weighted_avg_price,
    count(distinct stock_id) as stock_count,
    -- 若這天這個產業裡,任何一支股票是用 volume 加權,整體就標記為「混合/降級」
    case
        when count(distinct weighting_method) > 1 then 'mixed'
        else max(weighting_method)
    end as weighting_method
from stock_with_industry
group by trade_date, industry_code, market
    );
  