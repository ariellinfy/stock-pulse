-- dbt/models/marts/finance/fact_fear_greed.sql

{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'index_date', 'data_type': 'date'}
  )
}}

select
    index_date,
    fear_greed_score,
    fear_greed_rating
from {{ ref('stg_fear_greed') }}
{% if is_incremental() %}
where index_date >= (select date_sub(max(index_date), interval 3 day) from {{ this }})
{% endif %}
