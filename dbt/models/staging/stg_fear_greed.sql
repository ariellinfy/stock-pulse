-- Fear & Greed 總經指標 staging 層
select
    dt as index_date,
    score as fear_greed_score,
    fear_greed_rating
from {{ source('staging', 'raw_fear_greed') }}