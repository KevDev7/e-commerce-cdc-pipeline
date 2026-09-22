select encounter_id, patient_id, started_at, ended_at, encounter_class, total_claim_cost
from {{ ref('int_encounters_current') }}

