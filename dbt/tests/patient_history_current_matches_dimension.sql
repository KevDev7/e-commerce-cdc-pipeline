with active as (
    select patient_id, city, state, postal_code from {{ ref('dim_patient_history') }} where is_current
)
select coalesce(d.patient_id,h.patient_id) as patient_id
from {{ ref('dim_patients') }} d full outer join active h on d.patient_id=h.patient_id
where d.patient_id is null or h.patient_id is null
   or coalesce(d.city,'') <> coalesce(h.city,'')
   or coalesce(d.state,'') <> coalesce(h.state,'')
   or coalesce(d.postal_code,'') <> coalesce(h.postal_code,'')
