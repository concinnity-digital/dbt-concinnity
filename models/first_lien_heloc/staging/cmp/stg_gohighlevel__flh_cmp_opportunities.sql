with 

source as (

    select * from {{ source('gohighlevel', 'flh_cmp_opportunities') }}

),

renamed as (

    select
        string_field_0 as contact_id,
        string_field_1 as contactname,
        string_field_2 as opportuniy_id,
        string_field_4 as pipeline_name,
        string_field_5 as stage_name,
        string_field_6 as opportunity_assignedto,
        string_field_7 as status,
        string_field_8 as ambassador,
        string_field_9 as child_ambassador,
        string_field_10 as datecreated,
        string_field_11 as dateupdated

    from source

)

select * from renamed
