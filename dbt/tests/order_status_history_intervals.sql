select order_status_version_id from {{ ref('fct_order_status_history') }}
where source_order_to <= source_order_from or observed_to < observed_from
   or observed_duration_seconds < 0 or (is_deleted and is_current)
