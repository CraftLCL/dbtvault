{%- macro postgres__sat(src_pk, src_hashdiff, src_payload, src_eff, src_ldts, src_source, source_model) -%}

{{- dbtvault.check_required_parameters(src_pk=src_pk, src_hashdiff=src_hashdiff, src_payload=src_payload,
                                       src_ldts=src_ldts, src_source=src_source,
                                       source_model=source_model) -}}

{%- set src_pk = dbtvault.escape_column_names(src_pk) -%}
{%- set src_hashdiff = dbtvault.escape_column_names(src_hashdiff) -%}
{%- set src_payload = dbtvault.escape_column_names(src_payload) -%}
{%- set src_ldts = dbtvault.escape_column_names(src_ldts) -%}
{%- set src_source = dbtvault.escape_column_names(src_source) -%}

{%- set source_cols = dbtvault.expand_column_list(columns=[src_pk, src_hashdiff, src_payload, src_eff, src_ldts, src_source]) -%}
{%- set rank_cols = dbtvault.expand_column_list(columns=[src_pk, src_hashdiff, src_ldts]) -%}
{%- set pk_cols = dbtvault.expand_column_list(columns=[src_pk]) -%}

{%- if model.config.materialized == 'vault_insert_by_rank' %}
    {%- set source_cols_with_rank = source_cols + dbtvault.escape_column_names([config.get('rank_column')]) -%}
{%- endif -%}

{{ dbtvault.prepend_generated_by() }}

WITH source_data AS (
    {%- if model.config.materialized == 'vault_insert_by_rank' %}
    SELECT {{ dbtvault.prefix(source_cols_with_rank, 'a', alias_target='source') }}
    {%- else %}
    SELECT {{ dbtvault.prefix(source_cols, 'a', alias_target='source') }}
    {%- endif %}
    {# FROM dwd.{{source_model}} AS a #}
    FROM {{ ref(source_model) }} AS a 
    WHERE {{ dbtvault.multikey(src_pk, prefix='a', condition='IS NOT NULL') }}
    {%- if model.config.materialized == 'vault_insert_by_period' %}
    AND __PERIOD_FILTER__
    {% elif model.config.materialized == 'vault_insert_by_rank' %}
    AND __RANK_FILTER__
    {% endif %}
),

{% if dbtvault.is_any_incremental() %}

{# 
这种写法可以更新之前插入得数据，获得正确的ENDTIME 需要配置unique key 如hashdiff
distinct_source(
    SELECT DISTINCT {{ dbtvault.prefix(src_pk, 'a', alias_target='source') }}   FROM source_data a
),
all_source_data AS(
    SELECT {{ dbtvault.prefix(source_cols, 'a', alias_target='source') }}
    
    FROM {{ ref(source_model) }} AS a
    LEFT JOIN distinct_source
    ON {{ dbtvault.multikey([src_pk], prefix=['distinct_source','a'], condition='=') }}
    WHERE {{ dbtvault.multikey(src_pk, prefix='distinct_source', condition='IS NOT NULL') }}
)
, #}
latest_records AS (
    SELECT {{ dbtvault.prefix(rank_cols, 'a', alias_target='target') }}
    FROM
    (
        SELECT {{ dbtvault.prefix(rank_cols, 'source',
         alias_target='target') }}
        FROM {{ this }} AS source
        {# FROM all_source_data AS source #}
    ) AS a
),

{%- endif %}

records_to_insert AS (
    SELECT DISTINCT {{ dbtvault.alias_all(source_cols, 'stage') }}
    FROM source_data AS stage
    {%- if dbtvault.is_any_incremental() %}
        LEFT JOIN latest_records
        ON {{ dbtvault.multikey([src_hashdiff], prefix=['latest_records','stage'], condition='=') }}
        WHERE  {{ dbtvault.prefix([src_hashdiff], 'latest_records', alias_target='target') }} IS NULL
    {%- endif %}
)

SELECT * FROM records_to_insert

{%- endmacro -%}