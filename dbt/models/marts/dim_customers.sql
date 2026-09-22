{{ config(unique_key='customer_id',
    pre_hook="{{ cdc_prepare('customers', 'customer_id', false) }}",
    post_hook="{{ cdc_acknowledge() }}") }}

select customer_id, customer_unique_id, postal_code, city, state
from {{ ref('int_customers_current') }}
{{ cdc_filter('customer_id') }}
