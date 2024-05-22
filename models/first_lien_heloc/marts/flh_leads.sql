with opportunities as (
    select * from {{ ref('stg_gohighlevel__flh_cmp_opportunities') }}
),
flh_cmp_contacts as (
    select id,dateadded,phone,email,firstname,lastname,ambassador from {{ ref('stg_gohighlevel__flh_cmp_contacts') }}
),
leads as (

  select 
    opportunities.*,
    dense_rank() over (partition by contactname order by datecreated desc) as email_rank
  from opportunities
    
),

final as (
    select 
        distinct leads.* ,
        flh_cmp_contacts.*
    from leads
    left join flh_cmp_contacts
        on leads.id = flh_cmp_contacts.id
    where leads.email_rank = 1
)

select * from final