{{ config(unique_key='order_status_version_id',
    pre_hook="{{ cdc_prepare('orders', 'order_id', false) }}",
    post_hook="{{ cdc_acknowledge() }}") }}

select *, observed_to is null and not is_deleted as is_current,
       case when observed_to is not null and not is_deleted
            then {{ dbt.datediff('observed_from', 'observed_to', 'second') }}
       end as observed_duration_seconds
from {{ ref('int_order_status_history') }}
{{ cdc_filter('order_id') }}
