{%- macro get_current_table(tablename,pk,endtime) -%}
    (
        
        select * from (            
            {%- if pk is none -%}
                select *, 1 etl_row_number from {{ref(tablename)}}
            {%- else -%}
                SELECT *,row_number() over (partition by {{pk}} order by ts desc,loan_notification_id asc ) etl_row_number from
                {{ref(tablename)}}
                where to_timestamp(cast(ts as float) / 1000) <= {{endtime}}
            {%- endif -%}
        ) A where etl_row_number=1
    )
{%- endmacro -%}