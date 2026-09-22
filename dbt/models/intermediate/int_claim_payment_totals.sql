-- Aggregate financial entries to claim grain BEFORE joining to claims.
select claim_id,
       sum(case when transaction_type = 'CHARGE' then coalesce(amount, 0) else 0 end) as charge_total,
       sum(coalesce(payments, 0)) as payment_total,
       sum(coalesce(adjustments, 0)) as adjustment_total,
       count(*) as financial_entry_count
from {{ ref('int_claim_transactions_current') }}
group by claim_id

