with orders as (
    select * from {{ ref('int_orders_current') }}
), items as (
    select order_id, sum(price) as item_total, sum(freight_value) as freight_total,
           count(*) as item_count
    from {{ ref('int_order_items_current') }}
    group by order_id
), payments as (
    select order_id, sum(payment_value) as payment_total, count(*) as payment_count
    from {{ ref('int_order_payments_current') }}
    group by order_id
)
select o.order_id, o.customer_id, o.status, o.purchased_at, o.approved_at,
       o.carrier_delivered_at, o.customer_delivered_at, o.estimated_delivery_at,
       coalesce(i.item_total,0) as item_total, coalesce(i.freight_total,0) as freight_total,
       coalesce(i.item_total,0)+coalesce(i.freight_total,0) as order_total,
       coalesce(i.item_count,0) as item_count,
       coalesce(p.payment_total,0) as payment_total, coalesce(p.payment_count,0) as payment_count,
       i.order_id is not null as has_items, p.order_id is not null as has_payments
from orders o
left join items i on o.order_id=i.order_id
left join payments p on o.order_id=p.order_id
