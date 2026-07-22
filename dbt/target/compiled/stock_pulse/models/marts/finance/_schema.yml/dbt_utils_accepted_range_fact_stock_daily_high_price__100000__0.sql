

with meet_condition as(
  select *
  from (select * from `stock-pulse-260629`.`stockpulse_staging`.`fact_stock_daily` where high_price is not null) dbt_subquery
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not high_price >= 0
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not high_price <= 100000
)

select *
from validation_errors

