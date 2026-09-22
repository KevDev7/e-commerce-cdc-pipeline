select c.claim_id, c.patient_id, c.encounter_id, c.status,
       c.outstanding_primary, c.outstanding_secondary, c.outstanding_patient,
       coalesce(p.charge_total, 0) as charge_total,
       coalesce(p.payment_total, 0) as payment_total,
       coalesce(p.adjustment_total, 0) as adjustment_total,
       coalesce(p.financial_entry_count, 0) as financial_entry_count
from {{ ref('int_claims_current') }} c
left join {{ ref('int_claim_payment_totals') }} p on c.claim_id = p.claim_id

