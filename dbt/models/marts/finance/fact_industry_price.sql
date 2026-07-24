-- dbt/models/marts/finance/fact_industry_price.sql

{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'trade_date', 'data_type': 'date'}
  )
}}

with base as (
    select
        trade_date,
        industry_code,
        market,
        total_weight_basis,
        weighted_avg_price,
        stock_count,
        weighting_method
    from {{ ref('int_industry_daily_price') }}
    {% if is_incremental() %}
    where trade_date >= (select date_sub(max(trade_date), interval 3 day) from {{ this }})
    {% endif %}
)

select
    b.trade_date,
    b.industry_code,
    d.industry_name,
    b.market,
    b.total_weight_basis,
    b.weighted_avg_price,
    b.stock_count,
    b.weighting_method,
    case
        -- 只有當「今天」與「前一天」使用相同的加權方法時,才計算報酬率,
        -- 否則兩者數值基礎不一致,比較沒有意義(已實測發現產業指數from
        -- trade_volume 切換至 trade_value 加權時,產生 5 倍以上假性跳動)
        when b.weighting_method = lag(b.weighting_method) over (partition by b.industry_code, b.market order by b.trade_date)
        then round(
            (b.weighted_avg_price - lag(b.weighted_avg_price) over (partition by b.industry_code, b.market order by b.trade_date))
            / nullif(lag(b.weighted_avg_price) over (partition by b.industry_code, b.market order by b.trade_date), 0) * 100,
            2
        )
        else null
    end as daily_return_pct
from base b
left join {{ ref('dim_industry') }} d on b.industry_code = d.industry_code