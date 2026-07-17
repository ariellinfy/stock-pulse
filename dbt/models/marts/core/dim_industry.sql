-- dbt/models/marts/core/dim_industry.sql

{{
  config(
    materialized='table'
  )
}}

-- 已知限制: 目前僅有產業代碼(industry_code),沒有官方代碼對照名稱表,
-- 因此無法提供可讀的產業名稱,下游若需要人類可讀名稱需另外查證補充。
select distinct
    industry_code
from {{ ref('stg_stock_universe') }}
where industry_code is not null