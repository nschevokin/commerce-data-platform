select
  nullif(btrim(product_category_name), '')          as original_category,
  nullif(btrim(product_category_name_english), '')  as english_category
from {{ raw('category_translation') }}
