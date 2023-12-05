with 

source as (

    select * from {{ source('src_fast_fleet_active_invetory_lists', 'uhaul') }}

),

renamed as (

    select
        ff_part__ as ff_part_number,
        description,

    from source

)

select * from source
