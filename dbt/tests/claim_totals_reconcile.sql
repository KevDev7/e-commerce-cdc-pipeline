-- A direct independent aggregate must match claim-grain mart values.
with ledger as (
    select claim_id, sum(coalesce(payments,0)) as paid
    from {{ ref('fct_claim_transactions') }} group by claim_id
)
select c.claim_id from {{ ref('fct_claims') }} c
left join ledger l on c.claim_id=l.claim_id
where c.payment_total <> coalesce(l.paid,0)

