/* List the most popular five tags for all repositories */
select tag, count(distinct title) as usage_count
from {{ ref('common') }} 
group by 1
qualify row_number() over(order by usage_count desc)<=5