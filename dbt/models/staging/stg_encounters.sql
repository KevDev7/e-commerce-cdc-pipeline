select encounter_id, patient_id, started_at, ended_at, encounter_class,
       total_claim_cost, updated_at, _event_id, _op, _source_order, _commit_at, _is_snapshot
from {{ source('raw', 'encounters') }}

