select transaction_id, claim_id, patient_id, transaction_type, amount, posted_at,
       payments, adjustments, transfers, outstanding
from {{ ref('int_claim_transactions_current') }}

