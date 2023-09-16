with source as (
    select * from {{ source("src_fast_fleet_sales","2023_06") }}
),
renamed as (
    select
        cast(Date as date) as date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job__ as Job,
        cast(Truck__ as string) as Truck,
        Invoice__ as Invoice,
        Payment_Method,
        Shift,
        Lead_Source,
        Notes,
    from source
    where Date is not null
)

select * from renamed