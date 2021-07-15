{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ dbtvault_test.get_schema_name() }}
{%- endmacro %}

{% macro get_schema_name(custom_schema_name, node) -%}
    {{- adapter.dispatch('get_schema_name', 'dbtvault_test')() -}}
{%- endmacro %}

{%- macro snowflake__get_schema_name() -%}

    {%- set schema_name -%}
        {{- target.schema -}}_{{ env_var('SNOWFLAKE_DB_USER') }}{{ '_' ~ env_var('CIRCLE_BRANCH', '') | replace('-','_') | replace('.','_') | replace('/','_') if env_var('CIRCLE_BRANCH', '') -}}
        {{- '_' ~ env_var('CIRCLE_JOB', '') if env_var('CIRCLE_JOB', '') -}}
        {{- '_' ~ env_var('CIRCLE_NODE_INDEX', '') if env_var('CIRCLE_NODE_INDEX', '') -}}
    {%- endset -%}

    {% do return(schema_name) %}

{%- endmacro %}

{%- macro bigquery__get_schema_name() -%}

    {%- set schema_name -%}
        {{ env_var('GCP_DATASET') | upper }}-{{ env_var('GCP_USER') | upper }}
    {%- endset -%}

    {% do return(schema_name) %}

{%- endmacro %}