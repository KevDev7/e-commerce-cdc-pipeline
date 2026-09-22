select patient_id, birth_date, death_date, gender, city, state, postal_code,
       updated_at, _event_id, _op, _source_order, _commit_at, _is_snapshot
from {{ source('raw', 'patients') }}

