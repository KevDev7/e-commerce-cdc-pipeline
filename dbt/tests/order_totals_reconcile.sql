-- Compare independent aggregates; payment need not equal item + freight in real data.
with items as (
 select order_id,sum(price) as item_total,sum(freight_value) as freight_total,count(*) as n
 from {{ ref('fct_order_items') }} group by order_id
), payments as (
 select order_id,sum(payment_value) as payment_total,count(*) as n
 from {{ ref('fct_order_payments') }} group by order_id
)
select o.order_id from {{ ref('fct_orders') }} o
left join items i on o.order_id=i.order_id left join payments p on o.order_id=p.order_id
where o.item_total <> coalesce(i.item_total,0) or o.freight_total <> coalesce(i.freight_total,0)
   or o.payment_total <> coalesce(p.payment_total,0) or o.item_count <> coalesce(i.n,0)
   or o.payment_count <> coalesce(p.n,0)
