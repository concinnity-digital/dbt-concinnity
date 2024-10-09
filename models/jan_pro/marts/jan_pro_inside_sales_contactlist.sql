
with 

unioned as (

    select * from {{ ref('stg_src_jan_pro_inside_sales_contactlist__contacts_chattanooga') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_contactlist__contacts_knoxville') }}

)

select * from unioned
