{{ config(severity = 'warn') }}
select product_id, original_category from {{ ref('dim_products') }} where category_mapping_missing
