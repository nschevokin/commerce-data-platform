{{ config(enabled=var('source_mode') == 'bronze') }}
{{ bronze_latest('category_translation', ['product_category_name', 'product_category_name_english']) }}
