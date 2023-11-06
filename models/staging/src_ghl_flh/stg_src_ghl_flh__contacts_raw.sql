with 

source as (

    select * from {{ source('src_ghl_flh', 'contacts_raw') }}

),

renamed as (

    select
        contacts_id,
        contacts_locationid,
        contacts_contactname,
        contacts_firstname,
        contacts_lastname,
        contacts_companyname,
        contacts_email,
        contacts_phone,
        contacts_dnd,
        contacts_type,
        contacts_source,
        contacts_assignedto,
        contacts_city,
        contacts_state,
        contacts_postalcode,
        contacts_address1,
        contacts_dateadded,
        contacts_dateupdated,
        contacts_dateofbirth,
        contacts_tags,
        contacts_country,
        contacts_lastactivity,
        contacts_timezone,
        contacts_customfield_id,
        contacts_customfield_value,
        meta_total,
        meta_nextpageurl,
        meta_startafterid,
        meta_startafter,
        meta_currentpage,
        meta_nextpage

    from source

)

select * from renamed
