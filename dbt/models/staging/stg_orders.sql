select
  {{ clean_id('order_id') }}                          as order_id,
  {{ clean_id('customer_id') }}                       as customer_id,
  lower(btrim(order_status))                          as order_status,
  {{ strict_ts('order_purchase_timestamp') }}         as purchased_at,
  {{ strict_ts('order_approved_at') }}                as approved_at,
  {{ strict_ts('order_delivered_carrier_date') }}     as delivered_carrier_at,
  {{ strict_ts('order_delivered_customer_date') }}    as delivered_customer_at,
  {{ strict_ts('order_estimated_delivery_date') }}    as estimated_delivery_at
from {{ raw('orders') }}
