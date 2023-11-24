with 
contacts as (

    select 
        contacts_id,
        contacts_dateadded,
    from {{ ref('stg_src_ghl_flh__contacts_raw') }}

),
opportunities as (

    select * from {{ ref('stg_src_ghl_flh__raw_opportunities') }}
    
),
pipelines as (

    select * from {{ ref('stg_src_gohighlevel__pipelines') }}
    
),
raw as (

    SELECT 
        distinct 
        contacts_id,
        contacts_firstname as First_Name,
        contacts_lastname as Last_Name,
        INITCAP(concat(contacts_firstname, " ",contacts_lastname)) as contacts_fullName,
        contacts_email as Contacts_Email,
        contacts_source as Source,
        contacts_assignedto as AssignedTo
    FROM {{ ref('stg_src_ghl_flh__contacts_raw') }}
    
),
Amb_Source as (

    SELECT 
    distinct
        DENSE_RANK() OVER (PARTITION BY contacts_email ORDER BY contacts_dateUpdated DESC) as email_rank, 
        contacts_id,
        INITCAP(concat(contacts_firstname, " ",contacts_lastname)) as contacts_fullName,
        case when contacts_customfield_id IN ("UvjzjlAdNrorwQ0ZH1U4" ) then contacts_customfield_value end as Ambassador_Source
    FROM {{ ref('stg_src_ghl_flh__contacts_raw') }}
    WHERE contacts_customfield_id IN ("UvjzjlAdNrorwQ0ZH1U4")

),

How_did_you_hear AS (

    SELECT 
    distinct
        contacts_id as contacts_id2,
        INITCAP(concat(contacts_firstname, " ",contacts_lastname)) as contacts_fullName2,
        case when contacts_customfield_id IN ("bzq3CW9oBJSzZ6v0voII")  then contacts_customfield_value end as How_did_you_hear_about_us
    FROM {{ ref('stg_src_ghl_flh__contacts_raw') }}
    WHERE contacts_customfield_id IN ("bzq3CW9oBJSzZ6v0voII")

),
Referral_name AS (

    SELECT 
    distinct
        contacts_id as contacts_id3,
        INITCAP(concat(contacts_firstname, " ",contacts_lastname)) as contacts_fullName3,
        case when contacts_customfield_id IN ("zni40BEz2cLAV5jOikN1")  then contacts_customfield_value end as Referral_Name
    FROM {{ ref('stg_src_ghl_flh__contacts_raw') }}
    WHERE contacts_customfield_id IN ("zni40BEz2cLAV5jOikN1")

),
final as (

    select 
        contacts.contacts_id as contact_id,
        raw.First_Name as First_Name,
        raw.Last_Name as Last_Name,
        opportunities.opportunities_contact_name as name,
        opportunities.opportunities_contact_email as Contacts_Email,
        raw.Source,
        raw.AssignedTo,
        opportunities.opportunities_assignedto as opp_assigned_to,
        contacts.contacts_dateadded as date_created,
        opportunities.opportunities_status as Status,
        amb_source.Ambassador_Source,
        how_did_you_hear.How_did_you_hear_about_us,
        referral_name.Referral_Name,
        string_agg(pipelines.stage_name) as stage_name,
        string_agg(opportunities.opportunities_pipelineStageId) as StageId
    from contacts
    left join opportunities on opportunities.opportunities_contact_id = contacts.contacts_id
    left join raw  on raw.contacts_id = opportunities.opportunities_contact_id
    left join Amb_Source on amb_source.contacts_id = raw.contacts_id
    left join how_did_you_hear on how_did_you_hear.contacts_id2 = raw.contacts_id
    left join referral_name on raw.contacts_id = referral_name.contacts_id3
    left join  pipelines on pipelines.stage_id = opportunities.opportunities_pipelineStageId
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

select * from final