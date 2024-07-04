with 

source as (

    select * from {{ source('src_jan_pro_inside_sales_appointments', 'utah_is') }}

),

renamed as (

    select
        timestamp,
        date_worked,
        proposed_date_of_appointment,
        organization,
        address,
        inside_sales_staff,
        appointment_details_scope,
        notes

    from source

)

select * from renamed