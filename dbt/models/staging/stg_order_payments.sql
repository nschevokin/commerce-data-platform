select
  {{ clean_id('order_id') }}                 as order_id,
  {{ strict_int('payment_sequential') }}     as payment_sequential,
  btrim(payment_type)                        as payment_type,
  {{ strict_int('payment_installments') }}   as payment_installments,
  {{ strict_money('payment_value') }}        as recorded_payment_brl
from {{ raw('order_payments') }}
