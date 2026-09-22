{{ config(unique_key='order_item_key',
    pre_hook="{{ cdc_prepare('order_items', 'order_item_key', false) }}",
    post_hook="{{ cdc_acknowledge() }}") }}

-- Keep the canonical intermediate model in the tested build graph.
-- depends_on: {{ ref('int_order_items_current') }}
with affected_state as (
    {{ current_state('stg_order_items', 'order_item_key') }}
)
select order_item_key, order_id, order_item_id, product_id, seller_id, shipping_limit_at, price, freight_value
from affected_state
{{ cdc_filter('order_item_key') }}
