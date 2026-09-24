select customer_id, customer_unique_id, postal_code, city, state, updated_at
from (
    {{ current_state('stg_customers', 'customer_id') }}
) current_rows
