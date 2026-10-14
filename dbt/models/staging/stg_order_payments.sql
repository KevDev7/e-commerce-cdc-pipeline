select
    payment_key,
    order_id,
    cast(payment_sequential as integer) as payment_sequential,
    payment_type,
    cast(payment_installments as integer) as payment_installments,
    {{ source_money('payment_value') }} as payment_value,
    {{ source_timestamptz('updated_at') }} as updated_at,
    _event_id,
    _op,
    _source_order,
    _commit_at,
    _is_snapshot,
    _source_file
from (
    {{ deduplicate_events('order_payments') }}
) events
