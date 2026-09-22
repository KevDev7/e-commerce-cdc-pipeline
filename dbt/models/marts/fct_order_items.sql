select order_item_key, order_id, order_item_id, product_id, seller_id, shipping_limit_at, price, freight_value
from {{ ref('int_order_items_current') }}
