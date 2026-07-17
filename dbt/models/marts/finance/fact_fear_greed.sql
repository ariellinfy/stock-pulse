-- dbt/models/marts/finance/fact_fear_greed.sql

{{
  config(
    materialized='table'
  )
}}

select
    index_date,
    fear_greed_score,
    fear_greed_rating
from {{ ref('stg_fear_greed') }}