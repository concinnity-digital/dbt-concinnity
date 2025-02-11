with 

source as (

    select * from {{ source('src_jan_pro_inside_sales_call_logs', '2025_logs') }}

),

renamed as (

    select
        date,
        time,
        caller,
        territory,
        call_status,
        call_duration,
        vertical,
        call_disposition

    from source

)

select * from renamed
