with o as (
  select coalesce(sum(merchandise_value_brl), 0) as m, coalesce(sum(freight_value_brl), 0) as f,
         coalesce(sum(merchandise_value_brl) filter (where is_eligible), 0) as em,
         coalesce(sum(freight_value_brl) filter (where is_eligible), 0) as ef
  from {{ ref('fct_orders') }}
), i as (
  select coalesce(sum(i.price_brl), 0) as m, coalesce(sum(i.freight_brl), 0) as f,
         coalesce(sum(i.price_brl) filter (where fo.is_eligible), 0) as em,
         coalesce(sum(i.freight_brl) filter (where fo.is_eligible), 0) as ef
  from {{ ref('fct_order_items') }} i join {{ ref('fct_orders') }} fo using (order_id)
)
select o.*, i.* from o, i
where o.m <> i.m or o.f <> i.f or o.em <> i.em or o.ef <> i.ef
