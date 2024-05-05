with contacts as (
    select * from {{ ref('stg_gohighlevel__flh_cmp_contacts') }}
),
flh_cmp_opportunities as (
    select contact_id_opportunities,stage_name,pipeline_name,status,ambassador_source from {{ ref('stg_gohighlevel__flh_cmp_opportunities') }}
),
leads as (

  select 
    contacts.*,
    dense_rank() over (partition by email order by dateupdated desc) as email_rank
  from contacts
    
),

final as (
    select 
        distinct leads.* ,
        flh_cmp_opportunities.*
    from leads
    left join flh_cmp_opportunities 
        on leads.id = flh_cmp_opportunities.contact_id_opportunities
    where leads.email_rank = 1
)

select * from final