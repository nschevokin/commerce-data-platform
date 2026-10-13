select
  {{ clean_id('customer_id') }}                            as customer_id,
  {{ clean_id('customer_unique_id') }}                     as customer_unique_id,
  {{ postal_prefix('customer_zip_code_prefix') }}          as postal_prefix,
  customer_zip_code_prefix                                 as postal_prefix_raw,
  coalesce(nullif(btrim(customer_city), ''), '(unknown)')  as city,
  coalesce(upper(nullif(btrim(customer_state), '')), '(unknown)') as state,
  nullif(btrim(customer_city), '') is null                 as city_unknown,
  nullif(btrim(customer_state), '') is null                as state_unknown
from {{ raw('customers') }}
