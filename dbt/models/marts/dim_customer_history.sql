{{ config(unique_key='customer_version_id',
    pre_hook="{{ cdc_prepare('customers', 'customer_id', false) }}",
    post_hook="{{ cdc_acknowledge() }}") }}

-- Keep the canonical intermediate model in the tested build graph.
-- depends_on: {{ ref('int_customer_history') }}
select *, observed_to is null and not is_deleted as is_current
from (
    {{ customer_history() }}
) affected_state
{{ cdc_filter('customer_id') }}
