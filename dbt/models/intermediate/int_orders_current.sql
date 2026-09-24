select order_id, customer_id, status, purchased_at, approved_at, carrier_delivered_at, customer_delivered_at, estimated_delivery_at, updated_at
from (
    {{ current_state('stg_orders', 'order_id') }}
) current_rows
