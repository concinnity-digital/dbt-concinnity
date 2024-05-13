{{
    config(
        unique_key = "surrogate_key",
        partition_by ={ "field": "date", "data_type": "date" }
    )
}}

with 

unioned as (
    select * from {{ ref('stg_src_jan_pro_lsa__detroit') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__east') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__minneapolis') }}
    
)

select * from unioned
