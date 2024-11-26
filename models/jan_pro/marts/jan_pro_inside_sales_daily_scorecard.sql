with 

source as (

    select * from {{ source('src_jan_pro_inside_sales_scorecard', 'daily_scorecard') }}

),

renamed as (

    select
        date,
        metric_name,
        target,
        actual

    from source

)

select * from renamed
