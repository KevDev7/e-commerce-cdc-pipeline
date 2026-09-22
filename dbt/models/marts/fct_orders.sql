select o.order_id, o.customer_id, o.status, o.purchased_at, o.approved_at,
       o.carrier_delivered_at, o.customer_delivered_at, o.estimated_delivery_at,
       coalesce(i.item_total,0) as item_total, coalesce(i.freight_total,0) as freight_total,
       coalesce(i.item_total,0)+coalesce(i.freight_total,0) as order_total,
       coalesce(i.item_count,0) as item_count,
       coalesce(p.payment_total,0) as payment_total, coalesce(p.payment_count,0) as payment_count,
       i.order_id is not null as has_items, p.order_id is not null as has_payments
from {{ ref('int_orders_current') }} o
left join {{ ref('int_order_item_totals') }} i on o.order_id=i.order_id
left join {{ ref('int_order_payment_totals') }} p on o.order_id=p.order_id
