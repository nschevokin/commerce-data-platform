select
  {{ clean_id('seller_id') }}                              as seller_id,
  {{ postal_prefix('seller_zip_code_prefix') }}            as postal_prefix,
  seller_zip_code_prefix                                   as postal_prefix_raw,
  coalesce(nullif(btrim(seller_city), ''), '(unknown)')    as city,
  coalesce(upper(nullif(btrim(seller_state), '')), '(unknown)') as state
from {{ raw('sellers') }}
