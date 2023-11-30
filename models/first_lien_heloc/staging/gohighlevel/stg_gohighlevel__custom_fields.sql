with 

source as (

    select * from {{ source('gohighlevel', 'custom_fields') }}

),

renamed as (

    select
        id,
        name,
        fieldkey,
        placeholder,
        datatype,
        position,
        picklistoptions,
        ismultifileallowed,
        maxfilelimit

    from source

)

select * from renamed
