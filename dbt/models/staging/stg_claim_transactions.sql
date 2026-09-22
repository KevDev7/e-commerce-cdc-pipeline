select transaction_id, claim_id, patient_id, transaction_type, amount, posted_at,
       payments, adjustments, transfers, outstanding, updated_at,
       _event_id, _op, _source_order, _commit_at, _is_snapshot
from {{ source('raw', 'claim_transactions') }}

