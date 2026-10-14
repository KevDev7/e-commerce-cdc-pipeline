select
    order_item_key,
    order_id,
    cast(order_item_id as integer) as order_item_id,
    product_id,
    seller_id,
    {{ source_timestamp('shipping_limit_at') }} as shipping_limit_at,
    {{ source_money('price') }} as price,
    {{ source_money('freight_value') }} as freight_value,
    {{ source_timestamptz('updated_at') }} as updated_at,
    _event_id,
    _op,
    _source_order,
    _commit_at,
    _is_snapshot,
    _source_file
from (
    {{ deduplicate_events('order_items') }}
) events
