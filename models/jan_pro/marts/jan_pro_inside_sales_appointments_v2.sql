
with 

unioned as (

    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_detroit') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_east') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_everline') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_houston') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_nashville') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_pittsburgh') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_sacramento') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_tulsa') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_utah') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_capital_district') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_wisconsin') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_missouri') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_philadelphia') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_sarasota') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_appointments__appointments_san_antonio') }}

)

select * from unioned
