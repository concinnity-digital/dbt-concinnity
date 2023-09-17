with source as (
    select * from {{ source("src_fast_fleet_mapping","region_mapping") }}
),
renamed as (
    select
        Truck,
        Region,
        Start_Date
    from source

)

select * from renamed