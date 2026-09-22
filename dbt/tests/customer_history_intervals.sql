select customer_version_id from {{ ref('dim_customer_history') }}
where source_order_to <= source_order_from or observed_to < observed_from
