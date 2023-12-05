with 

source as (

    select * from {{ source('src_fast_fleet_active_invetory_lists', 'nuts_bolts_and_washers') }}

),

renamed as (

    select
        ff_part__ as ff_part_number,
        description,
        primary_vendor

    from source

)

select * from source