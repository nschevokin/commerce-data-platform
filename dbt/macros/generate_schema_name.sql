{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set s = var('candidate_schema') -%}
  {%- if s is none or not modules.re.match('^cand_[0-9a-f]{32}$', s) -%}
    {{ exceptions.raise_compiler_error("candidate_schema must match cand_<32 hex>, got: " ~ s) }}
  {%- endif -%}
  {{ s }}
{%- endmacro %}
