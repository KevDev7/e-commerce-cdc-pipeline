{% macro current_state(model, primary_key) %}
with ranked as (
    select *, row_number() over (
        partition by {{ primary_key }} order by _source_order desc, _event_id desc
    ) as state_rank
    from {{ ref(model) }}
)
select * from ranked where state_rank = 1 and _op <> 'D'
{% endmacro %}

