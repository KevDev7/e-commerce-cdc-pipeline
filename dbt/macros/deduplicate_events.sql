{% macro deduplicate_events(table) %}
select * from (
    select *, row_number() over (
        partition by _event_id order by _source_file
    ) as delivery_rank
    from {{ source('raw', table) }}
) deliveries where delivery_rank = 1
{% endmacro %}
