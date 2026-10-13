select
  {{ clean_id('product_id') }}                   as product_id,
  nullif(btrim(product_category_name), '')       as original_category,
  {{ strict_int('product_weight_g') }}           as weight_g
from {{ raw('products') }}
