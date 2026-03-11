{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set env_type = env_var('DBT_ENV_TYPE', 'DEV') | upper -%}

  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- elif env_type == 'PROD' -%}
    {{ custom_schema_name | trim }}
  {%- else -%}
    {{ target.schema }}
  {%- endif -%}
{%- endmacro %}
