select
  {{ clean_id('order_id') }}                 as order_id,
  {{ strict_int('order_item_id') }}          as order_item_id,
  {{ clean_id('product_id') }}               as product_id,
  {{ clean_id('seller_id') }}                as seller_id,
  {{ strict_ts('shipping_limit_date') }}     as shipping_limit_at,
  {{ strict_money('price') }}                as price_brl,
  {{ strict_money('freight_value') }}        as freight_brl
from {{ raw('order_items') }}
