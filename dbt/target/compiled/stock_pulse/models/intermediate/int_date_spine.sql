-- dbt/models/intermediate/int_date_spine.sql



with date_range as (
    select
        min(trade_date) as min_date,
        max(trade_date) as max_date
    from `stock-pulse-260629`.`stockpulse_staging`.`stg_twse_tpex_daily`
)

select
    date_day as calendar_date
from unnest(
    generate_date_array(
        (select min_date from date_range),
        (select max_date from date_range),
        interval 1 day
    )
) as date_day