{{ config(enabled=var('source_mode') == 'bronze') }}
{{ bronze_latest('customers', ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state']) }}
