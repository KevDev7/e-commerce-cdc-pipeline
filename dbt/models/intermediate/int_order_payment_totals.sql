select order_id, sum(payment_value) as payment_total, count(*) as payment_count
from {{ ref('int_order_payments_current') }} group by order_id
