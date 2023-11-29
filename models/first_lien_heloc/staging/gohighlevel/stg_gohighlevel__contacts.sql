with 

source as (

    select * from {{ source('gohighlevel', 'contacts') }}

),

renamed as (

    select
        id,
        locationid,
        contactname,
        firstname,
        lastname,
        companyname,
        email,
        phone,
        dnd,
        type,
        source.source,
        assignedto,
        city,
        state,
        postalcode,
        address1,
        dateadded,
        dateupdated,
        dateofbirth,
        tags,
        country,
        lastactivity,
        customfield,
        timezone

    from source

),
add_custom_field as (
    select 
        *,
        json_extract_scalar(custom_field, "$.id") as custom_field_id,
        json_extract_scalar(custom_field, "$.value") as custom_field_value
    from renamed
    left join unnest(json_extract_array(customfield)) as custom_field
)

select * from add_custom_field
