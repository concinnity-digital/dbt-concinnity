with 

source as (

    select * from {{ source('src_jan_pro_inside_sales_appointments', 'sacramento_is') }}

),

renamed as (

    select
        cast(timestamp as string) as timestamp,
        date_worked,
        proposed_date_of_appointment,
        organization,
        address,
        inside_sales_staff,
        appointment_details_scope,
        notes,
        cast(null as string) as account

    from source
    where timestamp is not null

)

select * from renamed
