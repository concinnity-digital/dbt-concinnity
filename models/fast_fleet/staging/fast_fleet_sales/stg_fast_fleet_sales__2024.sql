/* 
with source as (
    select * from {{ source("src_fast_fleet_sales","2024_01") }}
    union all
    select * from {{ source("src_fast_fleet_sales","2024_02") }}
    union all
    select * from {{ source("src_fast_fleet_sales","2024_03") }}
    union all
    select * from {{ source("src_fast_fleet_sales","2024_04") }}
    union all
    select * from {{ source("src_fast_fleet_sales","2024_05") }}
    union all
    select * from {{ source("src_fast_fleet_sales","2024_06") }}
    union all
    select * from {{ source("src_fast_fleet_sales","2024_07") }}
    union all
    select * from {{ source("src_fast_fleet_sales","2024_08") }}
    union all
    select * from {{ source("src_fast_fleet_sales","2024_09") }}
    union all
    select * from {{ source("src_fast_fleet_sales","2024_10") }}
    union all
    select * from {{ source("src_fast_fleet_sales","2024_11") }}
    union all
    select * from {{ source("src_fast_fleet_sales","2024_12") }}
    
),
renamed as (
    select
        safe_cast(Date as Date) as Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        cast(GP_Margin as string) as GP_Margin,
        Service_Type,
        Classification,
        Job__ as Job,
        Truck__ as Truck,
        Invoice__ as Invoice,
        Payment_Method,
        Shift,
        initcap(trim(Lead_Source)) as Lead_Source,
        Notes
    from source
    where Date is not null
)

select * from renamed

*/