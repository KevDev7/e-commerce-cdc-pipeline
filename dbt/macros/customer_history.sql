{% macro customer_history() %}
-- Track observed attribute changes, including tombstones. This cannot reconstruct
-- customer attributes from before the initial snapshot.
-- Snapshot timestamps are transfer times, not source commit times. If CDC has
-- already begun for this customer, that snapshot cannot establish an earlier
-- history version: it might already contain one of the later changes. Retain
-- it in raw/current-state processing, but start history with the captured CDC.
with timed_events as (
    select *, min(case when not _is_snapshot then _commit_at end)
        over (partition by customer_id) as first_cdc_at
    from {{ ref('stg_customers') }}
    {{ cdc_filter('customer_id') }}
), history_events as (
    select * from timed_events
    where not _is_snapshot or first_cdc_at is null or _commit_at < first_cdc_at
), fingerprints as (
    select *, md5(
        {% for column in ['customer_unique_id', 'city', 'state', 'postal_code'] %}
        coalesce(cast(length(cast({{ column }} as varchar)) as varchar) || ':' || cast({{ column }} as varchar), '-1:') ||
        {% endfor %}
        case when _op = 'D' then 'deleted' else 'present' end
    ) as attribute_hash
    from history_events
), previous as (
    select *, lag(attribute_hash) over (partition by customer_id order by _source_order) as previous_hash
    from fingerprints
), changes as (
    select * from previous where previous_hash is null or previous_hash <> attribute_hash
)
select md5(customer_id || ':' || cast(_source_order as varchar(35))) as customer_version_id,
       customer_id, customer_unique_id, city, state, postal_code,
       _commit_at as observed_from,
       lead(_commit_at) over (partition by customer_id order by _source_order) as observed_to,
       _source_order as source_order_from,
       lead(_source_order) over (partition by customer_id order by _source_order) as source_order_to,
       _op = 'D' as is_deleted,
       _is_snapshot as is_initial_snapshot
from changes
{% endmacro %}
