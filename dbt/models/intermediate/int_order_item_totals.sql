-- Aggregate to order grain before joining payments; avoid many-to-many fan-out.
select order_id, sum(price) as item_total, sum(freight_value) as freight_total,
       count(*) as item_count
from {{ ref('int_order_items_current') }} group by order_id
