-- dbt/models/marts/finance/fact_industry_price.sql

{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'trade_date', 'data_type': 'date'}
  )
}}

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
