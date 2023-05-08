{{ config(
    materialized='incremental',
    unique_key=['"id"', '"title"', '"body"', '"user"', '"tag"', '"repository"', '"status"', '"created_at"', '"merged_at"']
)}}

select "id"::int as "id",
"type", "title", "body", "user",
unnest("tags"::struct("id" varchar, "name" varchar)[]) as "tag",
"repository"::struct("owner" varchar, "name" varchar) as "repository", 
"status", 
case when "created_at"='null' then null else "created_at"::int end as "created_at",
case when "merged_at"='null' then null else "merged_at"::int end as "merged_at",
_insert_ts
from
(select jbody->>'$.id' as "id",
jbody->>'$.type' as "type",
jbody->>'$.title' as "title",
jbody->>'$.body' as "body",
jbody->>'$.user' as "user",
jbody->>'$.tags' as "tags",
jbody->>'$.status' as "status",
jbody->>'$.repository' as "repository",
jbody->>'$.merged_at' as "merged_at",
jbody->>'$.created_at' as "created_at",
_insert_ts
 from 
(select json(body) as jbody,
    _insert_ts 
    from {{source('message_landing', 'message_landing')}} ml
        {% if is_incremental() %}
        join (select max(_insert_ts) as max_insert_ts from {{this}}) a on 1=1
        where ml._insert_ts > a.max_insert_ts
        {% endif %})a
where jbody->>'$.type'='pull-requests')a