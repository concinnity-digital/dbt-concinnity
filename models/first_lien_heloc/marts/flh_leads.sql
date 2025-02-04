with
    opportunities as (
        select *
        from {{ ref("stg_gohighlevel__flh_cmp_opportunities") }}
    ),
    flh_cmp_contacts as (
        select id, dateadded, phone, email, firstname, lastname, ambassador_source, final_source
        from {{ ref("stg_gohighlevel__flh_cmp_contacts") }}
    ),
    leads as (
        select
            opportunities.*,
            dense_rank() over (
                partition by contactname order by datecreated desc
            ) as email_rank
        from opportunities
    ),
    final as (
        select 
            leads.*, 
            flh_cmp_contacts.*,
            count(*) over () as total_row_count
        from leads
        left join flh_cmp_contacts on leads.contact_id = flh_cmp_contacts.id
        where flh_cmp_contacts.id is not null
        --and leads.email_rank = 1 

    )

select * 
from final
