select payment_key, order_id, payment_sequential, payment_type, payment_installments, payment_value, updated_at, _event_id, _op, _source_order, _commit_at, _is_snapshot
from {{ source('raw', 'order_payments') }}
