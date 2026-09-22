with active as (
 select order_id, status, count(*) over (partition by order_id) as versions
 from {{ ref('fct_order_status_history') }} where is_current
)
select coalesce(o.order_id,h.order_id) as order_id
from {{ ref('fct_orders') }} o full outer join active h on o.order_id=h.order_id
where o.order_id is null or h.order_id is null or o.status <> h.status or h.versions <> 1
