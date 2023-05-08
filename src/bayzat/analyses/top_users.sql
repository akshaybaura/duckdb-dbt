/* List top users based on number of repositories they contributed  */
select "user", count(distinct repository) as contributed_repo_count
from {{ ref('common') }}
where type='pull-requests'
group by 1
qualify row_number() over(order by contributed_repo_count desc)<=10