{{ config(unique_key='payment_key',
    pre_hook="{{ cdc_prepare('order_payments', 'payment_key', false) }}",
    post_hook="{{ cdc_acknowledge() }}") }}

select payment_key, order_id, payment_sequential, payment_type, payment_installments, payment_value
from {{ ref('int_order_payments_current') }}
{{ cdc_filter('payment_key') }}
