select customer_id, customer_unique_id, postal_prefix, postal_prefix_raw, city, state,
       postal_prefix is null as postal_prefix_unknown, city_unknown, state_unknown
from {{ ref('stg_customers') }}
