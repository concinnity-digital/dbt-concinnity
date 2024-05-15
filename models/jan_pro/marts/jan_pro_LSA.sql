{{
    config(
        unique_key = "surrogate_key",
        partition_by ={ "field": "date", "data_type": "date" }
    )
}}

with 

unioned as (
    select * from {{ ref('stg_src_jan_pro_lsa__augusta') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__detroit') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__east') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__jacksonville') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__minneapolis') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__nashville') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__new_jersey') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__philadelphia') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__pittsburgh') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__raleigh') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__sacramento') }}
    --union all 
    --select * from {{ ref('stg_src_jan_pro_lsa__sela') }}
    --union all 
    --select * from {{ ref('stg_src_jan_pro_lsa__sne') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__southern_georgia') }}
    --union all 
    --select * from {{ ref('stg_src_jan_pro_lsa__utah') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_lsa__wisconsin') }}

    
)

select * from unioned
