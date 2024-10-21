with 

source as (

    select * from {{ source('src_jan_pro_inside_sales_contactlist', 'contacts_sacramento') }}

),

renamed as (

    select
        list_source,
        vertical,
        disposition,
        business_name,
        business_phone,
        address,
        gk_name,
        dm_name,
        dm_title,
        dm_phone,
        direct_phone,
        dm_email,
        notes,
        geo,
        business_type,
        business_size,
        current_cleaning,
        contract_expiration,
        _1st_contact,
        next_contact,
        latest_contact,
        index,
        "JP Sacramento" as location

    from source
        where business_name is not null

)

select * from renamed
