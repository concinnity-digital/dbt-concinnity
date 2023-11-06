with 

source as (

    select * from {{ source('src_mapping', 'flh_ambassador_list') }}

),

renamed as (

    select
        string_field_0 as ambassador,
        string_field_1 as amb

    from source

)

select * from renamed
