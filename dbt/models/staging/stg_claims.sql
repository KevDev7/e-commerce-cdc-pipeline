select claim_id, patient_id, encounter_id, status,
       outstanding_primary, outstanding_secondary, outstanding_patient,
       updated_at, _event_id, _op, _source_order, _commit_at, _is_snapshot
from {{ source('raw', 'claims') }}

