select customer_id, customer_unique_id, postal_code, city, state, updated_at, _event_id, _op, _source_order, _commit_at, _is_snapshot, _source_file
from (
    {{ deduplicate_events('customers') }}
) events
