
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
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__hampton_is') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__georgia_aiken_is') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__tulsa_is') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__houston_is') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__detroit_is') }}

)

select * from unioned
