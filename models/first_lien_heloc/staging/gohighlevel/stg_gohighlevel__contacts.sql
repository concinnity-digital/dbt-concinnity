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
        timezone

    from source

)

select * from renamed
