with source as (
    select * from {{ source("src_fast_fleet_sales","2022_10") }}
),
renamed as (
    select
        Date,
        _ as Customer,
        Amount,
        Labor as Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job__ as Job,
        trim(cast(Truck__ as string)) as Truck,
        Payment_Method,
        initcap(trim(Lead_Source)) as Lead_Source,
        Notes,
    from source
    where Date is not null
)

select * from renamed