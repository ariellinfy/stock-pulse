-- dbt/models/marts/finance/fact_industry_price.sql

{{
  config(
    materialized='table'
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