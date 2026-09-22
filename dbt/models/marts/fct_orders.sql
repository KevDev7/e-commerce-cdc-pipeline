{{ config(unique_key='order_id',
    pre_hook="{{ cdc_prepare('orders', 'order_id', true) }}",
    post_hook="{{ cdc_acknowledge() }}") }}

with orders as (
    select * from {{ ref('int_orders_current') }}
    {{ cdc_filter('order_id') }}
), items as (
    select * from {{ ref('int_order_item_totals') }}
    {{ cdc_filter('order_id') }}
), payments as (
    select * from {{ ref('int_order_payment_totals') }}
    {{ cdc_filter('order_id') }}
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
