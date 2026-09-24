select payment_key, order_id, payment_sequential, payment_type, payment_installments, payment_value, updated_at
from (
    {{ current_state('stg_order_payments', 'payment_key') }}
) current_rows
