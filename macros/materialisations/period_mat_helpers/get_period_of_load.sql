{%- macro get_period_of_load(period, offset, start_timestamp) -%}

    {% set macro = adapter.dispatch('get_period_of_load',
                                    'dbtvault')(period=period,
                                                offset=offset,
                                                start_timestamp=start_timestamp) %}

    {% do return(macro) %}
{%- endmacro %}




{%- macro default__get_period_of_load(period, offset, start_timestamp) -%}

    {% set period_of_load_sql -%}
        SELECT DATE_TRUNC('{{ period }}', DATEADD({{ period }}, {{ offset }}, TO_DATE('{{ start_timestamp }}'))) AS period_of_load
    {%- endset %}

    {% set period_of_load_dict = dbtvault.get_query_results_as_dict(period_of_load_sql) %}

    {% set period_of_load = period_of_load_dict['PERIOD_OF_LOAD'][0] | string %}

    {% do return(period_of_load) %}
{%- endmacro -%}




{%- macro bigquery__get_period_of_load(period, offset, start_timestamp) -%}

    {% set period_of_load_sql -%}
        SELECT DATE_TRUNC(DATE_ADD( DATE('{{start_timestamp}}'), INTERVAL {{ offset }} {{ period }}), {{ period }}  ) AS PERIOD_OF_LOAD
    {%- endset %}

    {% set period_of_load_dict = dbtvault.get_query_results_as_dict(period_of_load_sql) %}

    {% set period_of_load = period_of_load_dict['PERIOD_OF_LOAD'][0] | string %}

    {% do return(period_of_load) %}
{%- endmacro -%}




{%- macro sqlserver__get_period_of_load(period, offset, start_timestamp) -%}
    {#  MSSQL cannot CAST datetime2 strings with more than 7 decimal places #}
    {% set start_timestamp_mssql = start_timestamp[0:23] %}

    {% set period_of_load_sql -%}
        SELECT DATEADD({{ period }}, DATEDIFF({{period}}, 0, DATEADD({{ period }}, {{ offset }}, CAST('{{ start_timestamp_mssql }}' AS DATETIME2))), 0) AS period_of_load
    {%- endset %}

    {% set period_of_load_dict = dbtvault.get_query_results_as_dict(period_of_load_sql) %}

    {% set period_of_load = period_of_load_dict['PERIOD_OF_LOAD'][0] | string %}

    {% do return(period_of_load) %}
{%- endmacro -%}

{%- macro postgres__get_period_of_load(period, offset, start_timestamp) -%}

    {# {% set period_of_load_sql -%}
        SELECT DATE_TRUNC('{{ period }}', 
        {% if period=='year' %}
            '{{ start_timestamp }}'::timestamp + {{ offset }} *  interval '1 year'
        {% elif period=='month' %}
            '{{ start_timestamp }}'::timestamp + {{ offset }} *  interval '1 month'
        {% elif period=='day' %}
            '{{ start_timestamp }}'::timestamp + {{ offset }} *  interval '1 day'
        {% elif period=='hour' %}
            '{{ start_timestamp }}'::timestamp + {{ offset }} *  interval '1 hour'
        {% elif period=='minute' %}
            '{{ start_timestamp }}'::timestamp + {{ offset }} *  interval '1 minute'
        {% elif period=='second' %}
            '{{ start_timestamp }}'::timestamp + {{ offset }} *  interval '1 second'            
        {% else %}
            '9999-12-31 23:59:59.9999999'::timestamp
        {% endif %}       
        ) AS period_of_load
    {%- endset %} #}
    {% set period_of_load_sql -%}
    SELECT DATE_TRUNC('{{ period }}','{{ start_timestamp }}'::timestamp + interval '{{ offset }} {{ period }}'  ) AS period_of_load
    {%- endset %}
    {% set period_of_load_dict = dbtvault.get_query_results_as_dict(period_of_load_sql) %}

    {% set period_of_load = period_of_load_dict['period_of_load'][0] | string %}

    {% do return(period_of_load) %}
{%- endmacro -%}