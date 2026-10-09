with active as (
 select customer_id, customer_unique_id, city, state, postal_code from {{ ref('dim_customer_history') }} where is_current
)
select coalesce(d.customer_id,h.customer_id) as customer_id
from {{ ref('dim_customers') }} d full outer join active h on d.customer_id=h.customer_id
where d.customer_id is null or h.customer_id is null
{% for column in ['customer_unique_id', 'city', 'state', 'postal_code'] %}
 or d.{{ column }} <> h.{{ column }}
 or (d.{{ column }} is null and h.{{ column }} is not null)
 or (d.{{ column }} is not null and h.{{ column }} is null)
{% endfor %}
