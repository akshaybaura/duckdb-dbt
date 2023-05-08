{{ config(
    materialized='incremental',
    unique_key=['"id"', '"user"', '"label"', '"repository"', '"state"', '"started_at"', '"closed_at"']
    )}}

select "id"::int as "id",
"type", "title", "body", "user",
unnest("labels"::struct("id" varchar, "name" varchar)[]) as "label",
"repository", "state",
case when "started_at"='null' then null else "started_at"::timestamp end as "started_at",
case when "closed_at"='null' then null else "closed_at"::timestamp end as "closed_at",
_insert_ts
from
(select jbody->>'$.id' as "id",
jbody->>'$.type' as "type",
jbody->>'$.title' as "title",
jbody->>'$.body' as "body",
jbody->>'$.user' as "user",
jbody->>'$.labels' as "labels",
jbody->>'$.repository' as "repository",
jbody->>'$.state' as "state",
jbody->>'$.closed_at' as "closed_at",
jbody->>'$.started_at' as "started_at",
_insert_ts
 from 
(select json(body) as jbody,
    _insert_ts 
    from {{source('message_landing', 'message_landing')}} ml
        {% if is_incremental() %}
        join (select max(_insert_ts) as max_insert_ts from {{this}}) a on 1=1
        where ml._insert_ts > a.max_insert_ts
        {% endif %})a
where jbody->>'$.type'='issue')a