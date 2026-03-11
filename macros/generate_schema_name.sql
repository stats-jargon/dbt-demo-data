{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set is_dev = target.name in ['dev', 'default'] -%}

  {%- if is_dev -%}
    {{ target.schema }}
  {%- elif custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
