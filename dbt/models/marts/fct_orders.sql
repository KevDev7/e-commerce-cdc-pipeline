{{ config(unique_key='order_id',
    pre_hook="{{ cdc_prepare('orders', 'order_id', true) }}",
    post_hook="{{ cdc_acknowledge() }}") }}

-- Canonical intermediate views remain in the tested graph. Shared SQL filters
-- complete entity histories before ranking; merely filtering a view can rank all rows.
-- depends_on: {{ ref('int_order_creations') }}
-- depends_on: {{ ref('dim_customer_history') }}
-- depends_on: {{ ref('int_orders_current') }}
with creations as (
    {{ order_creations() }}
), orders as (
    {{ current_state('stg_orders', 'order_id') }}
), current_items as (
    {{ current_state('stg_order_items', 'order_item_key', order_details=true) }}
), current_payments as (
    {{ current_state('stg_order_payments', 'payment_key', order_details=true) }}
), items as (
    select order_id, sum(price) as item_total, sum(freight_value) as freight_total,
           count(*) as item_count
    from current_items
    {{ cdc_filter('order_id') }}
    group by order_id
), payments as (
    select order_id, sum(payment_value) as payment_total, count(*) as payment_count
    from current_payments
    {{ cdc_filter('order_id') }}
    group by order_id
)
select o.order_id, o.customer_id, o.status, o.purchased_at, o.approved_at,
       o.carrier_delivered_at, o.customer_delivered_at, o.estimated_delivery_at,
       coalesce(i.item_total,0) as item_total, coalesce(i.freight_total,0) as freight_total,
       coalesce(i.item_total,0)+coalesce(i.freight_total,0) as order_total,
       coalesce(i.item_count,0) as item_count,
       coalesce(p.payment_total,0) as payment_total, coalesce(p.payment_count,0) as payment_count,
       i.order_id is not null as has_items, p.order_id is not null as has_payments,
       c.captured_created_at, h.customer_version_id,
       case when c.order_id is null then 'creation_not_captured'
            when h.customer_version_id is null then 'customer_history_unavailable'
            else 'matched' end as customer_history_status
from orders o
left join items i on o.order_id=i.order_id
left join payments p on o.order_id=p.order_id
left join creations c on o.order_id=c.order_id
-- Use the canonical history view so a selected fact build sees all loaded events,
-- even when the materialized dimension has not yet processed its own checkpoint.
-- The dimension uses the same version IDs and precedes this fact in a full build.
left join {{ ref('int_customer_history') }} h
    on c.customer_id_at_creation=h.customer_id
   and h.source_order_from <= c.creation_source_order
   and (h.source_order_to is null or c.creation_source_order < h.source_order_to)
   and h.observed_from <= c.captured_created_at
   and not h.is_deleted
