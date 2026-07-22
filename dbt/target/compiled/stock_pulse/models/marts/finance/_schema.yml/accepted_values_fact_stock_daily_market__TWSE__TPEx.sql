
    
    

with all_values as (

    select
        market as value_field,
        count(*) as n_records

    from `stock-pulse-260629`.`stockpulse_staging`.`fact_stock_daily`
    group by market

)

select *
from all_values
where value_field not in (
    'TWSE','TPEx'
)


