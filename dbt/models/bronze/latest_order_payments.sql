{{ config(enabled=var('source_mode') == 'bronze') }}
{{ bronze_latest('order_payments', ['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value']) }}
