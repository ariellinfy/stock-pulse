-- dbt/tests/assert_no_mixed_weighting_method.sql
-- 一致性檢查: weighting_method 若為 'mixed',代表同一天同一產業裡,
-- 個股加權方式不一致(常見原因: 轉板股票、資料源交界邊界情況),
-- 這類異常應該被主動發現,而非等待人工偶然查詢才注意到
select *
from {{ ref('fact_industry_price') }}
where weighting_method = 'mixed'