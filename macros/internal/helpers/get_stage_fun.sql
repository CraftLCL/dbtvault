{%- macro get_load_datetime() -%}
    to_timestamp(cast(ts as float) / 1000) at time zone 'Asia/Shanghai' 
{%- endmacro -%}

{%- macro get_effective_from() -%}
    to_timestamp(cast(ts as float) / 1000) at time zone 'Asia/Shanghai'
{%- endmacro -%}

{%- macro get_start_date(default) -%}
    case when ts = {{default}} then '1999-1-01 1:00:00.000' else to_timestamp(cast(ts as float) / 1000) at time zone 'Asia/Shanghai' end
{%- endmacro -%}

{%- macro get_end_date(columns) -%}
    coalesce(to_timestamp(cast(lead(ts, 1, null) over ( partition by 
    {%- for column in columns -%}
        {%- if loop.last -%}
            {{- " {} ".format(column) -}}
        {%- else -%}
            {{- " {}".format(column)~"," -}}
        {%- endif -%}
    {%- endfor -%}
    order by ts) as float) / 1000) at time zone 'Asia/Shanghai','9999-12-31 23:59:59.999999')
{%- endmacro -%}