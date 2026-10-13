with item_totals as (
  select order_id,
         count(*)          as item_count,
         sum(price_brl)    as merchandise_value_brl,
         sum(freight_brl)  as freight_value_brl
  from {{ ref('fct_order_items') }}
  group by order_id
), payment_totals as (
  select order_id,
         count(*)                   as payment_row_count,
         sum(recorded_payment_brl)  as recorded_payment_brl
  from {{ ref('fct_payments') }}
  group by order_id
), orders as (
  select o.*, s.is_eligible, s.contract_version as status_contract_version
  from {{ ref('stg_orders') }} o
  join {{ ref('order_status_contract') }} s using (order_status)
)
select
  o.order_id,
  o.customer_id,
  o.order_status,
  o.is_eligible,
  o.status_contract_version,
  o.purchased_at,
  o.purchased_at::date                 as purchase_date,
  o.approved_at,
  o.delivered_carrier_at,
  o.delivered_customer_at,
  o.estimated_delivery_at,
  coalesce(i.item_count, 0)            as item_count,
  coalesce(p.payment_row_count, 0)     as payment_row_count,
  i.merchandise_value_brl,
  i.freight_value_brl,
  i.merchandise_value_brl + i.freight_value_brl as item_total_brl,
  p.recorded_payment_brl,
  p.recorded_payment_brl - (i.merchandise_value_brl + i.freight_value_brl) as payment_difference_brl,
  i.order_id is not null               as has_items,
  p.order_id is not null               as has_payments,
  c.customer_id is not null            as has_customer,
  o.delivered_customer_at is not null  as has_delivery_actual,
  o.estimated_delivery_at is not null  as has_delivery_estimate,
  case when o.order_status = 'delivered' and o.is_eligible
            and o.delivered_customer_at is not null and o.estimated_delivery_at is not null
       then o.delivered_customer_at::date <= o.estimated_delivery_at::date
  end                                  as is_on_time,
  case when o.order_status = 'delivered' and o.is_eligible and o.delivered_customer_at is not null
       then extract(epoch from o.delivered_customer_at - o.purchased_at)::bigint
  end                                  as delivery_lead_time_seconds
from orders o
left join item_totals i using (order_id)
left join payment_totals p using (order_id)
left join {{ ref('dim_customers') }} c using (customer_id)
