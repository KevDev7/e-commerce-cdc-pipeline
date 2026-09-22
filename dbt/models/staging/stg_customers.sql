select customer_id, customer_unique_id, postal_code, city, state, updated_at, _event_id, _op, _source_order, _commit_at, _is_snapshot
from {{ source('raw', 'customers') }}
