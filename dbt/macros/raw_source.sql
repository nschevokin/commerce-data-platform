{% macro raw(name) -%}
  {%- if var('source_mode') == 'bronze' -%}
    {{ ref('latest_' ~ name) }}
  {%- else -%}
    {{ source('raw', name) }}
  {%- endif -%}
{%- endmacro %}

{% macro bronze_latest(source_name, columns) -%}
  {%- if var('source_mode') == 'bronze' and (var('replay_version') is none or var('frontier_end') is none) -%}
    {{ exceptions.raise_compiler_error("bronze mode needs replay_version and frontier_end") }}
  {%- endif -%}
  {%- if var('source_mode') == 'bronze' and not (
        modules.re.match('^[a-z0-9][a-z0-9._-]{0,80}$', var('replay_version'))
        and modules.re.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$', var('frontier_end'))) -%}
    {{ exceptions.raise_compiler_error("replay_version or frontier_end has an unexpected format") }}
  {%- endif -%}
select
  entity_key,
  revision,
  available_at
  {%- for c in columns %},
  payload ->> '{{ c }}' as {{ c }}
  {%- endfor %}
from (
  select distinct on (entity_key) entity_key, revision, available_at, payload
  from bronze.source_events
  where replay_version = '{{ var("replay_version") }}'
    and source_name = '{{ source_name }}'
    and available_at < '{{ var("frontier_end") }}'::timestamptz
  order by entity_key, revision desc
) latest
{%- endmacro %}
