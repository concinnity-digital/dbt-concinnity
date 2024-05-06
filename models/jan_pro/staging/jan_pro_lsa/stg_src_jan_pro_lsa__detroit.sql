with 

source as (

    select * from {{ source('src_jan_pro_lsa', 'detroit') }}

),

renamed as (

    select
        date_received,
        lead_id,
        lead_type,
        charge_status,
        name_number,
        notes,
        dispute,
        dispute_status,
        remarks

    from source

)

select * from renamed
