select customer_version_id, customer_id, customer_unique_id, city, state, postal_code,
       observed_from, observed_to, source_order_from, source_order_to,
       is_deleted, is_initial_snapshot, is_current
from {{ ref('int_customer_history') }}
