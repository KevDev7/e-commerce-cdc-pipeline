{{ config(unique_key='payment_key',
    pre_hook="{{ cdc_prepare('order_payments', 'payment_key', false) }}",
    post_hook="{{ cdc_acknowledge() }}") }}

-- Keep the canonical intermediate model in the tested build graph.
-- depends_on: {{ ref('int_order_payments_current') }}
with affected_state as (
    {{ current_state('stg_order_payments', 'payment_key') }}
)
select payment_key, order_id, payment_sequential, payment_type, payment_installments, payment_value
from affected_state
{{ cdc_filter('payment_key') }}
