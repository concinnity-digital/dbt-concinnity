with raw as (

    SELECT 
        distinct 
        id as contacts_id,
        firstname as First_Name,
        lastname as Last_Name,
        INITCAP(concat(firstname, " ",lastname)) as contacts_fullName,
        email as Contacts_Email,
        source as Source,
        assignedto as AssignedTo
    FROM {{ ref('stg_src_ghl_flh__contacts_raw_v2') }}
    
),
Amb_Source as (

    SELECT 
    distinct
        DENSE_RANK() OVER (PARTITION BY email ORDER BY dateUpdated DESC) as email_rank, 
        id as contacts_id,
        INITCAP(concat(firstname, " ",lastname)) as contacts_fullName,
        case when customfield_id IN ("UvjzjlAdNrorwQ0ZH1U4" ) then customfield_value end as Ambassador_Source
    FROM {{ ref('stg_src_ghl_flh__contacts_raw_v2') }}
    WHERE customfield_id IN ("UvjzjlAdNrorwQ0ZH1U4")

),

How_did_you_hear AS (

    SELECT 
    distinct
        id as contacts_id2,
        INITCAP(concat(firstname, " ",lastname)) as contacts_fullName2,
        case when customfield_id IN ("bzq3CW9oBJSzZ6v0voII")  then customfield_value end as How_did_you_hear_about_us
    FROM {{ ref('stg_src_ghl_flh__contacts_raw_v2') }}
    WHERE customfield_id IN ("bzq3CW9oBJSzZ6v0voII")

),
Referral_name AS (

    SELECT 
    distinct
        id as contacts_id3,
        INITCAP(concat(firstname, " ",lastname)) as contacts_fullName3,
        case when customfield_id IN ("zni40BEz2cLAV5jOikN1")  then customfield_value end as Referral_Name
    FROM {{ ref('stg_src_ghl_flh__contacts_raw_v2') }}
    WHERE customfield_id IN ("zni40BEz2cLAV5jOikN1")

)

select 
    -- raw.contacts_id as Contact_Id,
    opp.contact_id as Contact_Id,
    raw.First_Name as First_Name,
    raw.Last_Name as Last_Name,
    -- raw.contacts_fullName as Name,
    opp.contact_name as name,
    -- raw.Contacts_Email,
    opp.contact_email as Contacts_Email,
    raw.Source,
    raw.AssignedTo,
    opp.assignedto as opp_assigned_to,
    opp.createdAt as Date_Created,
    opp.pipelineStageId as StageId,
    opp.status as Status,
    amb_source.Ambassador_Source,
    how_did_you_hear.How_did_you_hear_about_us,
    referral_name.Referral_Name,
    stage.stage_name
from {{ ref('stg_src_gohighlevel__opportunities') }} opp 
left join raw  on raw.contacts_id = opp.contact_id
left join Amb_Source on amb_source.contacts_id = raw.contacts_id
left join how_did_you_hear on how_did_you_hear.contacts_id2 = raw.contacts_id
left join referral_name on raw.contacts_id = referral_name.contacts_id3
LEFT JOIN {{ ref('stg_src_gohighlevel__pipelines') }} stage 
  on stage.stage_id = opp.pipelineStageId

