select
  p.product_id,
  p.original_category,
  t.english_category,
  coalesce(t.english_category, p.original_category, '(unknown)') as category_label,
  t.english_category is null as category_mapping_missing,
  p.weight_g
from {{ ref('stg_products') }} p
left join {{ ref('stg_category_translation') }} t using (original_category)
