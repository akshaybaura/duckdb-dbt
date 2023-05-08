# Solution to Assignment

## Sections
1. [Summary](#summary)  
2. [Replication steps](#replication)
3. [Common structure](#structure)
4. [Analytics SQL queries](#SQL)

## TL;DR version

![High Level architecture](/images/overview.jpeg)

This solution involves building a data pipeline that pulls data from an SQS queue using Python and dumps it into a landing table in DuckDB. The data is then transformed using dbt. 
This pipeline allows for efficient and automated handling of incoming stream data, enabling batch oriented transform of the raw data.  

## Replication

1. Run `docker-compose up -d`.
This step might take a few minutes.  
2. Run the suitable message producer script.  
3. Run `make all`.  

### Notes
1. The ingest step to poll from sqs may take few minutes although I have used async polling.  
2. Post ingest, dbt models are run and you can then see the data in finalized tables in csv format put into your host. Follow logs to get more info.  
3. Post dbt-docs step you can view the lineage graph on http://localhost:4444 and clicking on the blue icon on bottom-right of the page.  
You can see that **issues** and **pull_requests** tables are flattened tables built from raw messages table **message_landing**. Then there a common structure made in **common** table. Then there are 5 analyses queries also written from this common table as per requirement. These are just queries and not materialized entities.     
4. As a last step, you will see duckdb cli open up. You can run `.open bayzat` to access the bayzat database. Some other useful commands could be:
`show tables;`  
`describe tablename`  
and other analyses queries can be run here too.  

## Structure

![Common Table Schema](/images/common_table_schema.JPG)

### Notes
1. "tag" field in common table: Labels (for issues) and Tags (for PRs) are exploded into distinct rows per array item.    
2. "repository" field in common table": Repository name is substringed for issue events from url and fetched by name key for pull-requests events.  
3. "event_start_ts" field in common table": started_at for issue events and created_at for pr events.  
"event_end_ts" field in common table": closed_at for issue events and merged_at for pr events.  
4. _insert_ts is an internal audit column.  

## SQL 

1. List the most active users  

```
select "user" as most_active_users from 
"bayzat"."main"."common"
group by 1 
order by count(distinct id) desc
limit 10;
```  

2. List longest open event (for Issue from started_at to closed_at for PullRequest from created_at to merged_at )  

```
select type, id as event_id, title, coalesce(event_end_ts, timezone('UTC',current_timestamp))-event_start_ts as duration_open
from "bayzat"."main"."common"
qualify row_number() over(order by duration_open desc)=1;
```  
Note: I have used current UTC timestamp for end dates which are null.  

3. List the most popular five tags for all repositories (or label for Issue)  

```
select tag, count(distinct title) as usage_count
from "bayzat"."main"."common" 
group by 1
qualify row_number() over(order by usage_count desc)<=5;
```  

4. List the total completed event count per repository for a given period  

```
select repository, count(distinct id) 
from  "bayzat"."main"."common"
where state='closed' and event_start_ts>='2020-01-01' and event_end_ts<='2020-12-31'
group by 1;
```

5. List top users based on number of repositories they contributed  

```
select "user", count(distinct repository) as contributed_repo_count
from "bayzat"."main"."common"
where type='pull-requests'
group by 1
qualify row_number() over(order by contributed_repo_count desc)<=10;
```  

Note: You may see these queries in the dbt UI as well.  
