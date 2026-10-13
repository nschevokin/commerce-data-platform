{{ config(enabled=var('source_mode') == 'bronze') }}
{{ bronze_latest('products', ['product_id', 'product_category_name', 'product_weight_g']) }}
