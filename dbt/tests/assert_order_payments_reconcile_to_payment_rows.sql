with o as (
  select coalesce(sum(recorded_payment_brl), 0) as p,
         coalesce(sum(recorded_payment_brl) filter (where is_eligible), 0) as ep
  from {{ ref('fct_orders') }}
), r as (
  select coalesce(sum(p.recorded_payment_brl), 0) as p,
         coalesce(sum(p.recorded_payment_brl) filter (where fo.is_eligible), 0) as ep
  from {{ ref('fct_payments') }} p join {{ ref('fct_orders') }} fo using (order_id)
)
select o.*, r.* from o, r where o.p <> r.p or o.ep <> r.ep
