with 

source as (

    select * from {{ source('gohighlevel', 'flh_cmp_opportunities') }}

),

renamed as (

    select
        string_field_0 as contact_id,
        string_field_1 as contactname,
        string_field_2 as opportuniy_name,
        string_field_3 as pipeline_name,
        string_field_4 as stage_name,
        string_field_5 as opportunity_assignedto,
        string_field_6 as status,
        string_field_7 as ambassador,
        string_field_8 as child_ambassador,
        --cast(string_field_10 as date) as datecreated,
        PARSE_DATE('%m/%d/%Y', string_field_9) as datecreated,
        string_field_10 as dateupdated

    from source

)

select * from renamed
