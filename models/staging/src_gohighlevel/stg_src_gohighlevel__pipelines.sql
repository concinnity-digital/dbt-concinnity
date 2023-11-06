with 

source as (

    select * from {{ source('src_gohighlevel', 'pipelines') }}

),

renamed as (

    select
        source.id,
        source.name,
        stages.id as stage_id,
        stages.name as stage_name,
        locationid

    from source
    left join unnest(stages) as stages

)

select * from renamed
