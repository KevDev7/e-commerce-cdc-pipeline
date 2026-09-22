select patient_id, birth_date, death_date, gender, city, state, postal_code
from {{ ref('int_patients_current') }}

