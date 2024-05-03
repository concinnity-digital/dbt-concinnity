with 

source as (

    select * from {{ source('gohighlevel', 'flh_cmp_contacts') }}

),

renamed as (

    select
        contactname,
        id,
        firstname,
        lastname,
        companyname,
        phone,
        email,
        ambassador_source,
        child_ambassador,
        dateadded,
        dateupdated

    from source

)

select * from renamed
