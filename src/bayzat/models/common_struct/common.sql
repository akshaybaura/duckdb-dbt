{{ config(
    materialized='incremental'
)}}

select 
"id",
"type",
"title", 
"user", 
"label"."name" as tag, 
str_split("repository",'/')[-1] as repository,
"state",
"started_at" as event_start_ts,
"closed_at" as event_end_ts,
_insert_ts
from 
{{ ref('issues') }} issues
 {% if is_incremental() %}
        join (select max(_insert_ts) as max_insert_ts from {{this}}) a on 1=1
        where issues._insert_ts > a.max_insert_ts
        {% endif %}
union

select 
"id",
"type", 
"title",
"user", 
"tag"."name" as tag, 
"repository"."name" as repository,
"status",
to_timestamp("created_at") as event_start_ts,
to_timestamp("merged_at") as event_end_ts,
_insert_ts
from 
{{ ref('pull_requests') }} prs
 {% if is_incremental() %}
        join (select max(_insert_ts) as max_insert_ts from {{this}}) a on 1=1
        where prs._insert_ts > a.max_insert_ts
        {% endif %}