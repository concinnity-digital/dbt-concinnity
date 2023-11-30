with 
contacts as (

    select 
        id,
        dateadded,
    from {{ ref('stg_gohighlevel__contacts') }}

),
opportunities as (

    select * from {{ ref('stg_gohighlevel__opportunities') }}
    
),
pipelines as (

    select * from {{ ref('stg_gohighlevel__pipelines') }}
    
),
raw as (

    SELECT 
        distinct 
        id,
        firstname as First_Name,
        lastname as Last_Name,
        INITCAP(concat(firstname, " ",lastname)) as fullName,
        email as Email,
        source as Source,
        assignedto as AssignedTo
    FROM {{ ref('stg_gohighlevel__contacts') }}
    
),
Amb_Source as (

    SELECT 
    distinct
        DENSE_RANK() OVER (PARTITION BY email ORDER BY dateUpdated DESC) as email_rank, 
        id,
        INITCAP(concat(firstname, " ",lastname)) as fullName,
        case when custom_field_id IN ("UvjzjlAdNrorwQ0ZH1U4" ) then custom_field_value end as Ambassador_Source
    FROM {{ ref('stg_gohighlevel__contacts') }} as contacts
    left join {{ ref('stg_gohighlevel__contact_custom_field') }} as custom_field
    on custom_field.contact_id = contacts.id
    WHERE custom_field_id IN ("UvjzjlAdNrorwQ0ZH1U4")

),

How_did_you_hear AS (

    SELECT 
    distinct
        id as id2,
        INITCAP(concat(firstname, " ",lastname)) as fullName2,
        case when custom_field_id IN ("bzq3CW9oBJSzZ6v0voII")  then custom_field_value end as How_did_you_hear_about_us
    FROM {{ ref('stg_gohighlevel__contacts') }} as contacts
    left join {{ ref('stg_gohighlevel__contact_custom_field') }} as custom_field
    on custom_field.contact_id = contacts.id
    WHERE custom_field_id IN ("bzq3CW9oBJSzZ6v0voII")

),
Referral_name AS (

    SELECT 
    distinct
        id as id3,
        INITCAP(concat(firstname, " ",lastname)) as fullName3,
        case when custom_field_id IN ("zni40BEz2cLAV5jOikN1")  then custom_field_value end as Referral_Name
    FROM {{ ref('stg_gohighlevel__contacts') }} as contacts
    left join {{ ref('stg_gohighlevel__contact_custom_field') }} as custom_field
    on custom_field.contact_id = contacts.id
    WHERE custom_field_id IN ("zni40BEz2cLAV5jOikN1")

),
final as (

    select 
        contacts.id as contact_id,
        raw.First_Name as First_Name,
        raw.Last_Name as Last_Name,
        opportunities.contact_name as name,
        opportunities.contact_email as Email,
        raw.Source,
        raw.AssignedTo,
        opportunities.assignedto as opp_assigned_to,
        contacts.dateadded as date_created,
        opportunities.status as Status,
        amb_source.Ambassador_Source,
        how_did_you_hear.How_did_you_hear_about_us,
        referral_name.Referral_Name,
        string_agg(pipelines.stage_name) as stage_name,
        string_agg(opportunities.pipelineStageId) as StageId
    from contacts
    left join opportunities on opportunities.contact_id = contacts.id
    left join raw  on raw.id = opportunities.contact_id
    left join Amb_Source on amb_source.id = raw.id
    left join how_did_you_hear on how_did_you_hear.id2 = raw.id
    left join referral_name on raw.id = referral_name.id3
    left join  pipelines on pipelines.stage_id = opportunities.pipelineStageId
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

select * from final