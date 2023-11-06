with 

source as (

    select * from {{ source('src_ghl_flh', 'raw_opportunities') }}

),

renamed as (

    select
        opportunities_id,
        opportunities_name,
        opportunities_monetaryvalue,
        opportunities_pipelineid,
        opportunities_pipelinestageid,
        opportunities_pipelinestageuid,
        opportunities_status,
        opportunities_source,
        opportunities_laststatuschangeat,
        opportunities_createdat,
        opportunities_updatedat,
        opportunities_contact_id,
        opportunities_contact_name,
        opportunities_contact_email,
        opportunities_contact_tags,
        opportunities_assignedto,
        meta_total,
        meta_nextpageurl,
        meta_startafterid,
        meta_startafter,
        meta_currentpage,
        meta_nextpage

    from source

)

select * from renamed
