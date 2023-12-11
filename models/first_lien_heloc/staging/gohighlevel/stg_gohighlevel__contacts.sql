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
        timestamp_sub(TIMESTAMP(dateadded), INTERVAL 13 HOUR) as dateadded,
        timestamp_sub(TIMESTAMP(dateupdated), INTERVAL 13 HOUR) as dateupdated,
        dateofbirth,
        tags,
        country,
        lastactivity,
        timezone

    from source

)

select * from renamed
