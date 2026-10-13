{% macro strict_money(col) -%}
  case
    when nullif(btrim({{ col }}), '') is null then null
    when btrim({{ col }}) ~ '^-?[0-9]{1,16}(\.[0-9]{1,2})?$' then btrim({{ col }})::numeric(18, 2)
    else ('malformed money value: ' || {{ col }})::numeric
  end
{%- endmacro %}

{% macro strict_int(col) -%}
  case
    when nullif(btrim({{ col }}), '') is null then null
    when btrim({{ col }}) ~ '^-?[0-9]{1,9}$' then btrim({{ col }})::integer
    else ('malformed integer value: ' || {{ col }})::integer
  end
{%- endmacro %}

{% macro strict_ts(col) -%}
  case
    when nullif(btrim({{ col }}), '') is null then null
    when btrim({{ col }}) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
      then btrim({{ col }})::timestamp
    else ('malformed timestamp value: ' || {{ col }})::timestamp
  end
{%- endmacro %}

{% macro clean_id(col) -%}
  nullif(btrim({{ col }}), '')
{%- endmacro %}

{% macro postal_prefix(col) -%}
  case when btrim({{ col }}) ~ '^[0-9]{1,5}$' then lpad(btrim({{ col }}), 5, '0') end
{%- endmacro %}
