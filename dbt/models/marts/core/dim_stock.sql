-- dbt/models/marts/core/dim_stock.sql

{{
  config(
    materialized='table'
  )
}}

select
    stock_id,
    company_name,
    stock_name,
    industry_code,
    listing_date,
    market
from {{ ref('stg_stock_universe') }}