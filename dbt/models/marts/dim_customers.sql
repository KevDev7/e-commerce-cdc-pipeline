select customer_id, customer_unique_id, postal_code, city, state
from {{ ref('int_customers_current') }}
