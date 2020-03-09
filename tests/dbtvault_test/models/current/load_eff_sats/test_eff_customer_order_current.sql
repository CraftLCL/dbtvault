{{- config(materialized='incremental', schema='vlt', enabled=true, tags=['feature', 'current']) -}}

{{ dbtvault.eff_sat(var('src_pk'), var('src_dfk'), var('src_sfk'), var('src_ldts'),
                    var('src_eff_from'), var('src_eff_to'), var('src_source'), var('link'),
                    var('source'))                                                               }}