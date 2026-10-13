{{ config(enabled=var('source_mode') == 'bronze') }}
{{ bronze_latest('sellers', ['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state']) }}
