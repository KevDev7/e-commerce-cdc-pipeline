select order_item_key, order_id, order_item_id, product_id, seller_id, shipping_limit_at, price, freight_value, updated_at
from (
    {{ current_state('stg_order_items', 'order_item_key') }}
) current_rows
