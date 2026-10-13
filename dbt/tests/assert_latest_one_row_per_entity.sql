{{ config(enabled=var('source_mode') == 'bronze') }}

{% set relations = ['orders', 'customers', 'order_items', 'order_payments', 'products', 'sellers',
                    'category_translation'] %}
{% for r in relations %}
select '{{ r }}' as source_name, entity_key, count(*) as rows_for_key
from {{ ref('latest_' ~ r) }}
group by entity_key
having count(*) > 1 or entity_key is null
{% if not loop.last %}union all{% endif %}
{% endfor %}
