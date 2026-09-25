-- Only a captured INSERT proves creation time. A snapshot cannot supply it.
-- A reinsert starts a new lifecycle for a reused order key.
select order_id, customer_id as customer_id_at_creation,
       _source_order as creation_source_order, _commit_at as captured_created_at
from (
    select *, row_number() over (partition by order_id order by _source_order desc) as creation_rank
    from {{ ref('stg_orders') }}
    where _op = 'I' and not _is_snapshot
) inserts
where creation_rank = 1
