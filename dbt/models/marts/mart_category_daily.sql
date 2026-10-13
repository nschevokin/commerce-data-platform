select
  o.purchase_date,
  p.category_label,
  bool_or(p.category_mapping_missing)  as category_mapping_missing,
  count(*)                             as item_rows,
  count(distinct i.order_id)           as distinct_orders,
  sum(i.price_brl)::numeric(18, 2)     as merchandise_value_brl,
  sum(i.freight_brl)::numeric(18, 2)   as freight_value_brl
from {{ ref('fct_order_items') }} i
join {{ ref('fct_orders') }} o using (order_id)
join {{ ref('dim_products') }} p using (product_id)
where o.is_eligible
group by o.purchase_date, p.category_label
