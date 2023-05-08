/* List the most active users */
select "user" as most_active_users from 
{{ ref('common') }}
group by 1 
order by count(distinct id) desc
limit 10