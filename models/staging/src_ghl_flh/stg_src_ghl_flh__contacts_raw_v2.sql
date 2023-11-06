with 

source as (

    select 
        *
    from {{ source('src_ghl_flh', 'contacts_raw_v2') }}
    where string_field_0 not in ("contacts.id")

),

renamed as (

    select
        string_field_0 as id,
        string_field_1 as locationId,
        string_field_2 as contactName,
        string_field_3 as firstName,
        string_field_4 as lastName,
        string_field_5 as companyName,
        string_field_6 as email,
        string_field_7 as phone,
        string_field_8 as dnd,
        string_field_9 as type,
        string_field_10 as source,
        string_field_11 as assignedTo,
        string_field_12 as city,
        string_field_13 as state,
        string_field_14 as postalCode,
        string_field_15 as address1,
        string_field_16 as dateAdded,
        string_field_17 as dateUpdated,
        string_field_18 as dateOfBirth,
        string_field_19 as tags,
        string_field_20 as country,
        string_field_21 as lastActivity,
        string_field_22 as timezone,
        string_field_23 as customField_id,
        string_field_24 as customField_value,
        string_field_25 as total,
        string_field_26 as nextPageUrl,
        string_field_27 as startAfterId,
        string_field_28 as startAfter,
        string_field_29 as currentPage,
        string_field_30 as nextPage,
        string_field_31 as prevPage
    from source

)

select * from renamed
