{{
    config(
        unique_key = "surrogate_key",
        partition_by ={ "field": "date", "data_type": "date" }
    )
}}


with 

unioned as (

    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__east_tn_is') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__everline_is') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__nashville_is') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__raleigh_is') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__sacramento_is') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__utah_is') }}

)

select * from unioned
