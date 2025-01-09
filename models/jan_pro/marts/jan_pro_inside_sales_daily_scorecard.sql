with 

source as (

    select * from {{ source('src_jan_pro_inside_sales_scorecard', 'agents_daily_scorecard') }}

),

renamed as (

    select
        date,
        agent,
        metric_name,
        actual,
        notes

    from source

)

select * from renamed
