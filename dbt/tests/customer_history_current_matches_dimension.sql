with active as (
 select customer_id, city, state, postal_code from {{ ref('dim_customer_history') }} where is_current
)
select coalesce(d.customer_id,h.customer_id) as customer_id
from {{ ref('dim_customers') }} d full outer join active h on d.customer_id=h.customer_id
where d.customer_id is null or h.customer_id is null
 or coalesce(d.city,'') <> coalesce(h.city,'') or coalesce(d.state,'') <> coalesce(h.state,'')
 or coalesce(d.postal_code,'') <> coalesce(h.postal_code,'')
