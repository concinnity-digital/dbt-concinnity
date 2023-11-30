with 

source as (

    select * from {{ source('gohighlevel', 'contacts') }}

),

renamed as (

    select
        id as contact_id,
        customfield
    from source

),
add_custom_field as (
    select 
        contact_id,
        json_extract_scalar(custom_field, "$.id") as custom_field_id,
        json_extract_scalar(custom_field, "$.value") as custom_field_value
    from renamed
    left join unnest(json_extract_array(customfield)) as custom_field
)

select * from add_custom_field
