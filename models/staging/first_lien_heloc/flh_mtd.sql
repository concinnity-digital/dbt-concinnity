with Source as (
    select 
        s.contact_id,
        s.Source,
        a.amb as amb_source
    FROM {{ ref('fth_mtd_leads') }} s
    left join {{ ref('stg_src_mapping__flh_ambassador_list') }} a on Lower(ltrim(rtrim(s.Source))) like Lower(ltrim(rtrim(concat('%',a.ambassador, '%'))))
), 
Amb_source as (
    select 
        s.contact_id,
        s.Ambassador_Source,
        a.amb as amb_as
    FROM {{ ref('fth_mtd_leads') }} s
    left join {{ ref('stg_src_mapping__flh_ambassador_list') }} a on Lower(ltrim(rtrim(s.Ambassador_Source))) like Lower(ltrim(rtrim(concat('%',a.ambassador, '%'))))
),
Referral as (
    select 
        s.contact_id,
        s.Referral_Name,
        a.amb as amb_ref
    FROM {{ ref('fth_mtd_leads') }} s
    left join {{ ref('stg_src_mapping__flh_ambassador_list') }} a on Lower(ltrim(rtrim(s.Referral_Name))) like Lower(ltrim(rtrim(concat('%',a.ambassador, '%'))))
),
How_did_you_hear_about_us as (
    select 
        s.contact_id,
        s.How_did_you_hear_about_us,
        a.amb as amb_how
    FROM {{ ref('fth_mtd_leads') }} s
    left join {{ ref('stg_src_mapping__flh_ambassador_list') }} a on Lower(ltrim(rtrim(s.How_did_you_hear_about_us))) like Lower(ltrim(rtrim(concat('%',a.ambassador, '%'))))
)

select
    s.Contact_Id,
    s.Name,
    s.First_Name,
    s.Last_Name,
    s.Source,
    so.amb_source,
    s.How_did_you_hear_about_us,
    h.amb_how,
    s.Ambassador_Source,
    a.amb_as,
    s.Referral_Name,
    r.amb_ref,
    s.Date_Created,
    s.AssignedTo,
    s.StageId,
    s.Status,
    s.stage_name

from {{ ref('fth_mtd_leads') }} s left join Source so on s.Contact_Id = so.Contact_Id
left join How_did_you_hear_about_us h on s.Contact_Id = h.Contact_Id
left join Amb_source a on s.Contact_Id = a.Contact_Id
left join Referral r on s.Contact_Id = r.Contact_Id