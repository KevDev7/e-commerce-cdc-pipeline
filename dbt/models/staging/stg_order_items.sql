select order_item_key, order_id, order_item_id, product_id, seller_id, shipping_limit_at, price, freight_value, updated_at, _event_id, _op, _source_order, _commit_at, _is_snapshot
from {{ source('raw', 'order_items') }}
