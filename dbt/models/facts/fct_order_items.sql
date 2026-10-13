select order_id, order_item_id, product_id, seller_id, shipping_limit_at, price_brl, freight_brl
from {{ ref('stg_order_items') }}
