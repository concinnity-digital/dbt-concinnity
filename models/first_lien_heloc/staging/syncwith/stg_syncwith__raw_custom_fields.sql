{{config(materialized = 'table')}}

with 

source as (

    select * from {{ source('syncwith', 'raw_custom_fields') }}

),

renamed as (

    select
        id,
        name,
        fieldkey,
        placeholder,
        datatype,
        position,
        picklistoptions

    from source

)

select * from renamed
