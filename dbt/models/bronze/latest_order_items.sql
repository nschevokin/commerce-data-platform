{{ config(enabled=var('source_mode') == 'bronze') }}
{{ bronze_latest('order_items', ['order_id', 'order_item_id', 'product_id', 'seller_id', 'shipping_limit_date', 'price', 'freight_value']) }}
