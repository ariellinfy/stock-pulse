-- dbt/models/marts/core/dim_industry.sql

{{
  config(
    materialized='table'
  )
}}

select distinct
    u.industry_code,
    m.industry_name
from {{ ref('stg_stock_universe') }} u
left join {{ ref('industry_code_mapping') }} m
    on u.industry_code = m.industry_code
where u.industry_code is not null