with f as (
  select coalesce(sum(merchandise_value_brl) filter (where is_eligible), 0) as m,
         coalesce(sum(recorded_payment_brl) filter (where is_eligible), 0) as p,
         count(*) filter (where is_eligible) as n
  from {{ ref('fct_orders') }}
), s as (
  select coalesce(sum(merchandise_value_brl), 0) as m, coalesce(sum(recorded_payment_brl), 0) as p,
         coalesce(sum(eligible_orders), 0) as n
  from {{ ref('mart_sales_daily') }}
), c as (
  select coalesce(sum(merchandise_value_brl), 0) as m from {{ ref('mart_category_daily') }}
)
select f.m, s.m, c.m, f.p, s.p, f.n, s.n from f, s, c
where f.m <> s.m or f.m <> c.m or f.p <> s.p or f.n <> s.n
