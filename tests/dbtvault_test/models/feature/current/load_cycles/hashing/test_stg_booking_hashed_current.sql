{{- config(materialized='table', schema='vlt', enabled=true, tags=['load_cycles_current', 'current'])                                            -}}

{%- set source_table = source('test_current', 'stg_booking_current')                                                                            -%}

{{ dbtvault.multi_hash([('BOOKING_REF', 'BOOKING_PK'),
                         ('CUSTOMER_ID', 'CUSTOMER_PK'),
                         (['CUSTOMER_ID', 'BOOKING_REF'],'CUSTOMER_BOOKING_PK'),
                         (['CUSTOMER_ID', 'NATIONALITY', 'PHONE'], 'BOOK_CUSTOMER_HASHDIFF', true),
                         (['BOOKING_REF', 'BOOKING_DATE', 'DEPARTURE_DATE', 'PRICE', 'DESTINATION'],
                         'BOOK_BOOKING_HASHDIFF', true)]) -}},

{{ dbtvault.add_columns(source_table,
                        [('!STG_BOOKING', 'SOURCE'),
                         ('BOOKING_DATE', 'EFFECTIVE_FROM')])                                                                    }}

{{ dbtvault.from(source_table)                                                                                                   }}