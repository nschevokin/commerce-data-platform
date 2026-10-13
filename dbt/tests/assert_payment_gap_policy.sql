{{ config(severity = 'error' if var('exact_fixture') else 'warn') }}
select order_id, payment_difference_brl
from {{ ref('fct_orders') }}
where has_items and has_payments and payment_difference_brl <> 0
