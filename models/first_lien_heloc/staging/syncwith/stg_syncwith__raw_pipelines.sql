{{config(materialized = 'table')}}

with 

source as (

    select * from {{ source('syncwith', 'raw_pipelines') }}

),

renamed as (

    select
        row,
        id,
        name,
        stages_id,
        stages_name,
        locationid

    from source

)

select * from renamed
