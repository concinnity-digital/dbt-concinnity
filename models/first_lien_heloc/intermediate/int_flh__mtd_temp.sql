with a as (
    select 
        *,
        case 
            when amb_source is not null then Source
            when amb_source is null and How_did_you_hear_about_us is not null then How_did_you_hear_about_us
            when How_did_you_hear_about_us is not null  then How_did_you_hear_about_us
            when How_did_you_hear_about_us is null then Source
            else Source
        end as Real_Source
    from  {{ ref('int_flh__mtd') }}
)

select 
    a.*, 
    case 
        when a.Ambassador_Source is not null then a.Ambassador_Source
        when a.Ambassador_Source is null then a.Real_Source 
        else a.Real_Source
    end as Real_Source2
from a
