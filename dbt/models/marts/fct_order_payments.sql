select payment_key, order_id, payment_sequential, payment_type, payment_installments, payment_value
from {{ ref('int_order_payments_current') }}
