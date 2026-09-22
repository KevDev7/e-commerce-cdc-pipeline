{{ config(unique_key='order_status_version_id',
    pre_hook="{{ cdc_prepare('orders', 'order_id', false) }}",
    post_hook="{{ cdc_acknowledge() }}") }}

-- Keep the canonical intermediate model in the tested build graph.
-- depends_on: {{ ref('int_order_status_history') }}
-- Redshift DATEDIFF needs TIMESTAMP inputs. UTC conversion preserves elapsed time across DST.
select *, observed_to is null and not is_deleted as is_current,
       case when observed_to is not null and not is_deleted
            then {{ dbt.datediff("timezone('UTC', observed_from)", "timezone('UTC', observed_to)", 'second') }}
       end as observed_duration_seconds
from (
    {{ order_status_history() }}
) affected_state
{{ cdc_filter('order_id') }}
