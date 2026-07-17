-- dbt/models/marts/core/dim_date.sql

{{
  config(
    materialized='table'
  )
}}

select
    calendar_date,
    extract(year from calendar_date) as year,
    extract(month from calendar_date) as month,
    extract(day from calendar_date) as day,
    extract(dayofweek from calendar_date) as day_of_week,
    format_date('%A', calendar_date) as day_name,
    case when extract(dayofweek from calendar_date) in (1, 7) then true else false end as is_weekend
from {{ ref('int_date_spine') }}