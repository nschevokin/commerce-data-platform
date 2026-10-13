select order_id, payment_sequential, payment_type, payment_installments, recorded_payment_brl
from {{ ref('stg_order_payments') }}
