select order_id, customer_id, status, purchased_at, approved_at, carrier_delivered_at, customer_delivered_at, estimated_delivery_at, updated_at, _event_id, _op, _source_order, _commit_at, _is_snapshot, _source_file
from (
    {{ deduplicate_events('orders') }}
) events
