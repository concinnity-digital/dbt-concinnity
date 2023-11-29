with 

source as (

    select * from {{ source('gohighlevel', 'opportunities') }}

),

renamed as (

    select
        id,
        name,
        monetaryvalue,
        pipelineid,
        pipelinestageid,
        pipelinestageuid,
        assignedto,
        status,
        source.source,
        laststatuschangeat,
        createdat,
        updatedat,
        contact_id,
        contact_name,
        contact_email,
        contact_phone,
        contact_tags,
        contact_companyname

    from source

)

select * from renamed
