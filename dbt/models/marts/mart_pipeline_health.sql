{% set rid = var('release_id') %}
{% set rv = var('replay_version') %}
{% set fe = var('frontier_end') %}
{% set parts = [('orders', 'stg_orders'), ('customers', 'stg_customers'), ('order_items', 'stg_order_items'),
                ('order_payments', 'stg_order_payments'), ('products', 'stg_products'),
                ('sellers', 'stg_sellers'), ('category_translation', 'stg_category_translation')] %}
{% for name, rel in parts %}
select
  {{ "'" ~ rid ~ "'::uuid" if rid else "null::uuid" }}                  as release_id,
  '{{ name }}'::text                                                     as source_name,
  {{ "'" ~ rv ~ "'::text" if rv else "null::text" }}                    as replay_version,
  {{ "'" ~ fe ~ "'::timestamptz" if fe else "null::timestamptz" }}      as frontier_end,
  count(*)::bigint                                                       as latest_rows,
  null::integer                                                          as blocking_failures,
  null::integer                                                          as warnings
from {{ ref(rel) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
