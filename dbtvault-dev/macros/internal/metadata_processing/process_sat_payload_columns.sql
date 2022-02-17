{%- macro process_sat_payload_columns(payload_columns=none, source_model=none) -%}

    {%- set payload_columns_to_select = [] -%}

    {% if source_model is mapping and source_model is not none -%}
        {%- set source_name = source_model | first -%}
        {%- set source_table_name = source_model[source_name] -%}
        {%- set source_relation = source(source_name, source_table_name) -%}
        {%- set all_source_columns = dbtvault.source_columns(source_relation=source_relation) -%}
    {%- elif source_model is not mapping and source_model is not none -%}
        {%- set source_relation = ref(source_model) -%}
        {%- set all_source_columns = dbtvault.source_columns(source_relation=source_relation) -%}
    {%- else -%}
        {%- set all_source_columns = [] -%}
    {%- endif -%}

    {%- if payload_columns is mapping -%}
        {%- if payload_columns.exclude_columns -%}
            {%- set payload_columns_to_select = dbtvault.process_columns_to_select(all_source_columns, payload_columns.columns) -%}
        {%- else -%}
            {%- set payload_columns_to_select = payload_columns.columns -%}
        {%- endif -%}
    {%- else -%}
        {%- set payload_columns_to_select = payload_columns -%}
    {%- endif -%}

    {%- do return(payload_columns_to_select) -%}

{%- endmacro -%}
