with 

source as (

    select * from {{ source('src_fast_fleet_active_invetory_lists', 'coolants_oils_and_shop_supplies') }}

),

renamed as (

    select
        ff__ as ff_part_number,
        description,
        vendor,
        primary_vendor__ as primary_vendor_number,
        secondary_vendor__ as secondary_number,
        purchase_price,
        sale_price

    from source

),
calculated_fields as (
    select
        renamed.*,
        safe_divide((sale_price - purchase_price), purchase_price) as mark_up,
    from renamed
)

select * from calculated_fields