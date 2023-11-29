with temptable as (
    select 
        a.*
        , b.amb as Real_Source2_amb
    FROM {{ ref('int_flh__mtd_temp') }} a
    left join {{ ref('stg_mapping__flh_ambassador_list') }} b on Lower(ltrim(rtrim(a.Real_Source2))) like Lower(ltrim(rtrim(concat('%',b.ambassador, '%'))))
),
final as( 

    select 
        a.Contact_id,
        a.Name,
        a.First_Name,
        a.Last_Name,
        a.AssignedTo,
        a.Source,
        a.How_did_you_hear_about_us,
        a.Ambassador_Source,
        a.Referral_Name,
        a.Status,
        a.stage_name,
        a.Date_Created,
        case 
            when b.Real_Source2_amb is not null then a.Real_Source2
            when b.Real_Source2_amb is null and a.amb_ref is not null then a.Referral_Name
            when a.amb_ref is null then a.Real_Source2
            else a.Real_Source2
        end as Final_Source
    from {{ ref('int_flh__mtd_temp') }} a 
    left join temptable b on a.Contact_Id = b.Contact_Id
)

select distinct * from final
where contact_id is not null