with 

source as (

    select * from {{ source('src_jan_pro_inside_sales_appointments', 'east_tn_is') }}

),

renamed as (

    select
        timestamp as string,
        account,
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
