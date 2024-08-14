{{ config(
    materialized='table'
) }}


with source as (
    select 
        *,
        trim(split(string_field_0, ' at')[0]) as date1,
        trim(split(string_field_0, ' at')[1]) as time,
        "JP Pittsburgh" as location
    from {{ source('src_jan_pro_lsa', 'pittsburgh') }}
),

renamed as (
    select
        PARSE_DATE('%B %d, %Y', CONCAT(SUBSTRING(date1, 1, INSTR(date1, ' ') - 1), ' ', SUBSTRING(date1, INSTR(date1, ' ') + 1, INSTR(date1, ',') - INSTR(date1, ' ') - 1), ', ', RIGHT(date1, 4))) AS date,
        time,
        string_field_0 as date_received,
        string_field_1 as lead_id,
        string_field_2 as lead_type,
        string_field_3 as charge_status,
        string_field_4 as name_number,
        string_field_5 as notes,
        string_field_6 as intent_of_the_call,
        cast(string_field_7 as string) as dispute,
        string_field_8 as dispute_status,
        string_field_9 as remarks,
        location
    from source
    where string_field_0 is not null
)

select * from renamed