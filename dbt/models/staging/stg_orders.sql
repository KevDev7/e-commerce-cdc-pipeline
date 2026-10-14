select
    order_id,
    customer_id,
    status,
    {{ source_timestamp('purchased_at') }} as purchased_at,
    {{ source_timestamp('approved_at') }} as approved_at,
    {{ source_timestamp('carrier_delivered_at') }} as carrier_delivered_at,
    {{ source_timestamp('customer_delivered_at') }} as customer_delivered_at,
    {{ source_timestamp('estimated_delivery_at') }} as estimated_delivery_at,
    {{ source_timestamptz('updated_at') }} as updated_at,
    _event_id,
    _op,
    _source_order,
    _commit_at,
    _is_snapshot,
    _source_file
from (
    {{ deduplicate_events('orders') }}
) events
