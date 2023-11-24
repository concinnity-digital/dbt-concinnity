{{
    config(enabled = false)
}}

with 

source as (

    select * from {{ source('src_fast_fleet_active_invetory_lists', 'belts_and_tensioners') }}

),

renamed as (

    select
        ff_part__ as ff_part_number,
        description,
        primary_vendor,
        primary_vendor__ as primary_vendor_number,
        secondary__ as secondary_number,
        purchase_price,
        sale_price,
        --safe_divide((sale_price - purchase_price), purchase_price) as mark_up,
        t__wh_or_both,
        min__truck_qty_,
        truck_max,
        min_wh_qty_

    from source

),
calculated_fields as (
    select
        renamed.*,
        safe_divide((sale_price - purchase_price), purchase_price) as mark_up,
    from renamed
)

select * from calculated_fields