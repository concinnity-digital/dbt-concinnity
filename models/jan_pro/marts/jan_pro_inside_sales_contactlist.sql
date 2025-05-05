
with 

unioned as (

    select * from {{ ref('stg_src_jan_pro_inside_sales_contactlist__contacts_chattanooga') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_contactlist__contacts_knoxville') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_contactlist__contacts_detroit') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_contactlist__contacts_everline') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_contactlist__contacts_houston') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_contactlist__contacts_tulsa') }}
    union all 
    select * from {{ ref('stg_src_jan_pro_inside_sales_contactlist__contacts_utah') }}

)

select * from unioned
