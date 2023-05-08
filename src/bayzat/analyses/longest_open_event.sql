/* List longest open event */
select type, id as event_id, title, coalesce(event_end_ts, timezone('UTC',current_timestamp))-event_start_ts as duration_open
from {{ ref('common') }}
qualify row_number() over(order by duration_open desc)=1

