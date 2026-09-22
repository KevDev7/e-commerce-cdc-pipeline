{{ config(unique_key='customer_version_id',
    pre_hook="{{ cdc_prepare('customers', 'customer_id', false) }}",
    post_hook="{{ cdc_acknowledge() }}") }}

select *, observed_to is null and not is_deleted as is_current
from {{ ref('int_customer_history') }}
{{ cdc_filter('customer_id') }}
