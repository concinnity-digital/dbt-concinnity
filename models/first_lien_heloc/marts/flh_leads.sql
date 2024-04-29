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
flh_cmp_opportunities as (
    select contact_id_opportunities,stage_name,pipeline_name,status from {{ ref('stg_gohighlevel__flh_cmp_opportunities') }}
),
leads as (

  select 
    dense_rank() over (partition by email order by dateupdated desc) as email_rank, 
    contacts.* except (tags, source),    
    case 
        when custom_field_id IN ("XsBMwip7fKtXHoFfqdoa" ,"zni40BEz2cLAV5jOikN1","UvjzjlAdNrorwQ0ZH1U4") AND custom_field_value = "Zach Ohlmoen" then "Zach Oehlman"
        when custom_field_id IN ("XsBMwip7fKtXHoFfqdoa" ,"zni40BEz2cLAV5jOikN1","UvjzjlAdNrorwQ0ZH1U4") AND custom_field_value = "Sebastien  Boyer" then "Sébastien Boyer"
        when custom_field_id IN ("XsBMwip7fKtXHoFfqdoa" ,"zni40BEz2cLAV5jOikN1","UvjzjlAdNrorwQ0ZH1U4") then TRIM(INITCAP(custom_field_value)) 
    end as Ambassador
  FROM contacts
  left join contact_customfield
    on contact_customfield.contact_id = contacts.id
  where custom_field_id IN ("UvjzjlAdNrorwQ0ZH1U4")
    and contains_substr(tags, "ambassadorlead") 
),

child_ambassador AS (

    SELECT 
        contacts.* except (source), 
        custom_field_value
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
        opportunities.* except (Id, name, assignedto,source,status),
        --pipelines.* except (Id, Name, locationid),
        flh_cmp_opportunities.*,
        custom_field_value as child_ambassador
    from leads
    left join opportunities 
        on leads.id = opportunities.contact_id
    --LEFT JOIN pipelines 
    --    on pipelines.stage_Id = opportunities.pipelineStageId
    left join flh_cmp_opportunities 
        on leads.id = flh_cmp_opportunities.contact_id_opportunities
    LEFT JOIN child_ambassador 
        on leads.id = child_ambassador.id
    where leads.email_rank = 1
)

select * from final