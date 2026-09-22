{% macro order_status_history() %}
-- Observation history starts at capture, not at the historical purchase date.
-- Exclude ambiguous overlapping snapshot baselines, as in customer history.
with timed as (
    select *, min(case when not _is_snapshot then _commit_at end)
        over (partition by order_id) as first_cdc_at
    from {{ ref('stg_orders') }}
    {{ cdc_filter('order_id') }}
), eligible as (
    select * from timed
    where not _is_snapshot or first_cdc_at is null or _commit_at < first_cdc_at
), previous as (
    select *, lag(status) over (partition by order_id order by _source_order) as previous_status,
        lag(_op = 'D') over (partition by order_id order by _source_order) as previously_deleted
    from eligible
), changes as (
    select * from previous
    where previously_deleted is null or status <> previous_status
        or (_op = 'D') <> previously_deleted
)
select md5(order_id || ':' || cast(_source_order as varchar(35))) as order_status_version_id,
       order_id, status, _commit_at as observed_from,
       lead(_commit_at) over (partition by order_id order by _source_order) as observed_to,
       _source_order as source_order_from,
       lead(_source_order) over (partition by order_id order by _source_order) as source_order_to,
       _op = 'D' as is_deleted, _is_snapshot as is_initial_snapshot
from changes
{% endmacro %}
