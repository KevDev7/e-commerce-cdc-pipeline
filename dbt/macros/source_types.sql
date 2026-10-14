{% macro source_money(column) %}
{# Reject excess fractional precision instead of silently rounding money. #}
cast(case
    when cast({{ column }} as decimal(38,18)) = cast({{ column }} as numeric(14,2))
    then {{ column }}
    when {{ column }} is null then null
    else 'INVALID_MONEY_SCALE'
end as numeric(14,2))
{% endmacro %}

{% macro source_timestamp(column) %}
{# Source business timestamps have no timezone; do not silently discard one. #}
cast(case when {{ column }} ~ '[ T][0-9]{2}:[0-9]{2}.*([zZ]|[+-][0-9]{2}(:?[0-9]{2})?)$'
    then 'INVALID_TIMESTAMP_TIMEZONE' else {{ column }} end as timestamp)
{% endmacro %}

{% macro source_timestamptz(column) %}
{# DMS timezone-less timestamps represent UTC, independent of query session. #}
case when {{ column }} ~ '[ T][0-9]{2}:[0-9]{2}.*([zZ]|[+-][0-9]{2}(:?[0-9]{2})?)$'
    then cast({{ column }} as timestamptz)
    else cast({{ column }} as timestamp) at time zone 'UTC' end
{% endmacro %}
