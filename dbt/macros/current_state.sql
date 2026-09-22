{% macro current_state(model, primary_key, order_details=false) %}
select * from (
    select *, row_number() over (
        partition by {{ primary_key }} order by _source_order desc, _event_id desc
    ) as state_rank
    from {{ ref(model) }}
 {% if is_incremental() and order_details %}
 -- Filter by complete row-key histories, not by order_id before ranking: a detail
 -- might have had a different parent in an earlier event.
 where {{ primary_key }} in (
     select {{ primary_key }} from {{ ref(model) }}
     {{ cdc_filter('order_id') }}
 )
 {% else %}
 {{ cdc_filter(primary_key) }}
 {% endif %}
) ranked where state_rank = 1 and _op <> 'D'
{% endmacro %}

