select *, source_order_to is null and not is_deleted as is_current
from {{ ref('int_patient_history') }}

