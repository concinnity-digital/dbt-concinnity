with 

source as (

    select * from {{ source('src_jan_pro_inside_sales', 'everline_is') }}

),

renamed as (

    select
        safe_cast(timestamp as string) as timestamp,
        safe_cast(date_worked as Date) as date_worked,
        safe_cast(proposed_date_of_appointment as date) as proposed_date_of_appointment,
        organization,
        address,
        inside_sales_staff,
        appointment_details_scope,
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
        "JP Everline" as location

    from source
    where timestamp is not null
    or notes NOT LIKE '%test%' 
    or notes NOT LIKE '%TEST%'

)

select * from renamed
