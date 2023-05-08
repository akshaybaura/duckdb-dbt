/* List the total completed event count per repository for a given period */
select repository, count(distinct id) 
from  {{ ref('common') }}
where state='closed' and event_start_ts>='2020-01-01' and event_end_ts<='2020-12-31'
group by 1