with 

source as (

    select * from {{ source('src_jan_pro_inside_sales_appointments', 'sacramento_is') }}

),

renamed as (

    select
        cast(timestamp as string) as timestamp,
        cast(date_worked as Date) as date_worked,
        cast(proposed_date_of_appointment as date) as proposed_date_of_appointment,
        organization,
        address,
        inside_sales_staff,
        appointment_details_scope,
        notes,
        cast(null as string) as account,
        "JP Sacramento" as location

    from source
    where timestamp is not null
      AND notes NOT LIKE '%test%'
)

select * from renamed
