select
  purchase_date,
  count(*) filter (where is_eligible)                         as eligible_orders,
  count(*) filter (where not is_eligible)                     as excluded_orders,
  count(*) filter (where is_eligible and has_items)           as eligible_orders_with_items,
  count(*) filter (where is_eligible and not has_items)       as eligible_orders_missing_items,
  count(*) filter (where is_eligible and not has_payments)    as eligible_orders_missing_payments,
  coalesce(sum(merchandise_value_brl) filter (where is_eligible), 0)::numeric(18, 2) as merchandise_value_brl,
  coalesce(sum(freight_value_brl)     filter (where is_eligible), 0)::numeric(18, 2) as freight_value_brl,
  coalesce(sum(recorded_payment_brl)  filter (where is_eligible), 0)::numeric(18, 2) as recorded_payment_brl,
  round(sum(merchandise_value_brl) filter (where is_eligible and has_items)
        / nullif(count(*) filter (where is_eligible and has_items), 0), 2)::numeric(18, 2)
                                                              as average_order_value_brl
from {{ ref('fct_orders') }}
group by purchase_date
