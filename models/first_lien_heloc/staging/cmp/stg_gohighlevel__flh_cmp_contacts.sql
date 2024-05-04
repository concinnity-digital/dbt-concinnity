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
        cast(ambassador_source as string) as ambassador,
        child_ambassador,
        dateadded,
        dateupdated

    from source

)

select * from renamed
