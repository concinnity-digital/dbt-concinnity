with contacts as (
    select * from {{ ref('stg_gohighlevel__contacts') }}
),
contact_customfield as (
    select * from {{ ref('stg_gohighlevel__contact_custom_field') }}
),
opportunities as (
    select * except (contact_tags) from {{ ref('stg_gohighlevel__opportunities') }}
),
pipelines as (
    select * from {{ ref('stg_gohighlevel__pipelines') }}
),
custom_fields as (
    select * from {{ ref('stg_gohighlevel__custom_fields') }}
),
leads as (

  SELECT 
    DENSE_RANK() OVER (PARTITION BY email ORDER BY dateUpdated DESC) as email_rank, 
    contacts.* except (tags, source),
    
    case 
      when custom_field_id IN ("XsBMwip7fKtXHoFfqdoa" ,"zni40BEz2cLAV5jOikN1","UvjzjlAdNrorwQ0ZH1U4") AND custom_field_value = "Zach Ohlmoen" then "Zach Oehlman"
      when custom_field_id IN ("XsBMwip7fKtXHoFfqdoa" ,"zni40BEz2cLAV5jOikN1","UvjzjlAdNrorwQ0ZH1U4") AND custom_field_value = "Sebastien  Boyer" then "Sébastien Boyer"
      when custom_field_id IN ("XsBMwip7fKtXHoFfqdoa" ,"zni40BEz2cLAV5jOikN1","UvjzjlAdNrorwQ0ZH1U4") then TRIM(INITCAP(custom_field_value)) end as Ambassador,
  FROM contacts

  LEFT JOIN contact_customfield
  ON contact_customfield.contact_id = contacts.id

  WHERE custom_field_id IN ("UvjzjlAdNrorwQ0ZH1U4")
  and CONTAINS_SUBSTR(tags, "ambassadorlead") 
),

child_ambassador AS (

  SELECT 
    contacts.* except (source), 
    custom_fields.name 
  FROM contacts
  
  LEFT JOIN contact_customfield
  ON contact_customfield.contact_id = contacts.id

  LEFT JOIN custom_fields
  ON contact_customfield.custom_field_id = custom_fields.id
  WHERE custom_field_id IN ("TI9CjrQW6XvO1cQSdrS0")
),

final as (
    select 
    distinct leads.* ,
    opportunities.* except (Id, name, assignedto,source),
    pipelines.* except (Id, Name, locationid),
    child_ambassador.name as child_ambassador
    from leads
    left join opportunities 
    on leads.id = opportunities.contact_id
    LEFT JOIN pipelines 
    on pipelines.Id = opportunities.pipelineStageId
    LEFT JOIN child_ambassador on leads.id = child_ambassador.id
    where leads.email_rank = 1
)

select * from final