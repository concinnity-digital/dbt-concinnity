{{ config(
    materialized='table'
) }}


with source as (
    select 
        *,
        trim(split(date_received, ' at')[0]) as date1,
        trim(split(date_received, ' at')[1]) as time,
        "JP Augusta" as location
    from {{ source('src_jan_pro_lsa', 'augusta') }}
),

renamed as (
    select
        PARSE_DATE('%B %d, %Y', CONCAT(SUBSTRING(date1, 1, INSTR(date1, ' ') - 1), ' ', SUBSTRING(date1, INSTR(date1, ' ') + 1, INSTR(date1, ',') - INSTR(date1, ' ') - 1), ', ', RIGHT(date1, 4))) AS date,
        time,
        date_received,
        cast(lead_id as string) as lead_id,
        lead_type,
        charge_status,
        name_number,
        notes,
        intent_of_the_call,
        dispute,
        dispute_status,
        remarks,
        location
    from source
    where date_received is not null
)

select * from renamed