with 

source as (

    select * from {{ source('src_jan_pro_inside_sales_appointments', 'appointments_wisconsin') }}

),

renamed as (

    select
        cast(timestamp as string) as timestamp,
        type_of_appointment,
        cast(proposed_date_of_appointment as date) as proposed_date_of_appointment,
        proposed_time_of_appointment,
        ---PARSE_TIME('%I:%M:%S %p', proposed_time_of_appointment) AS proposed_time_of_appointment,
        company_name,
        company_address,
        company_size,
        contact_name,
        contact_job_title,
        company_contact_number,
        company_email_address,
        inside_sales_staff,
        notes,
        valid_ as valid,
        proposal_ as proposal,
        closed_ as closed,
        date_closed as date_closed,
        safe_cast(contract_revenue as integer) as contract_rev,
        appointment_commission,
        closed_sale_commission,
        paid_ as paid,
        cast(null as string) as account,
        "JP NE Wisconsin" as location

    from source
        where timestamp is not null
        or notes NOT LIKE '%test%' 
        or notes NOT LIKE '%TEST%'

    )

select * from renamed