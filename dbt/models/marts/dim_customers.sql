{{ config(unique_key='customer_id',
    pre_hook="{{ cdc_prepare('customers', 'customer_id', false) }}",
    post_hook="{{ cdc_acknowledge() }}") }}

-- Keep the canonical intermediate model in the tested build graph.
-- depends_on: {{ ref('int_customers_current') }}
with affected_state as (
    {{ current_state('stg_customers', 'customer_id') }}
)
select customer_id, customer_unique_id, postal_code, city, state
from affected_state
{{ cdc_filter('customer_id') }}
