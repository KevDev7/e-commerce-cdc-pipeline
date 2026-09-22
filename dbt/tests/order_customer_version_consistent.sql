-- A matched version must belong to the captured creation and cover its sequence.
select o.order_id
from {{ ref('fct_orders') }} o
left join {{ ref('int_order_creations') }} c on o.order_id=c.order_id
left join {{ ref('dim_customer_history') }} h on o.customer_version_id=h.customer_version_id
where (o.customer_history_status = 'matched' and (
    h.customer_version_id is null or c.order_id is null or h.is_deleted
    or h.customer_id <> c.customer_id_at_creation
    or h.source_order_from > c.creation_source_order
    or h.source_order_to <= c.creation_source_order
    or h.observed_from > c.captured_created_at))
   or (o.customer_history_status <> 'matched' and o.customer_version_id is not null)
   or (o.customer_history_status = 'creation_not_captured' and c.order_id is not null)
   or (o.customer_history_status <> 'creation_not_captured' and c.order_id is null)
