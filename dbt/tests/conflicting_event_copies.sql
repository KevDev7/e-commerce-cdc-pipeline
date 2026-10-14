-- Replayed copies can differ in source file, but not in event contents.
select 'customers' as source_table, _event_id
from (
    select distinct customer_id, customer_unique_id, postal_code, city, state, updated_at, _event_id, _op, _source_order, _commit_at, _is_snapshot
    from {{ source('raw', 'customers') }}
) versions
group by _event_id
having count(*) > 1
union all
select 'orders' as source_table, _event_id
from (
    select distinct order_id, customer_id, status, purchased_at, approved_at, carrier_delivered_at, customer_delivered_at, estimated_delivery_at, updated_at, _event_id, _op, _source_order, _commit_at, _is_snapshot
    from {{ source('raw', 'orders') }}
) versions
group by _event_id
having count(*) > 1
union all
select 'order_items' as source_table, _event_id
from (
    select distinct order_item_key, order_id, order_item_id, product_id, seller_id, shipping_limit_at, price, freight_value, updated_at, _event_id, _op, _source_order, _commit_at, _is_snapshot
    from {{ source('raw', 'order_items') }}
) versions
group by _event_id
having count(*) > 1
union all
select 'order_payments' as source_table, _event_id
from (
    select distinct payment_key, order_id, payment_sequential, payment_type, payment_installments, payment_value, updated_at, _event_id, _op, _source_order, _commit_at, _is_snapshot
    from {{ source('raw', 'order_payments') }}
) versions
group by _event_id
having count(*) > 1
