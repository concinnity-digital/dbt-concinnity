with source as (
    select * from {{ source("src_fast_fleet_sales","2023_04") }}
),
renamed as (
    select
        cast(kan as date) as Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job__ as Job,
        trim(cast(Truck__ as string)) as Truck,
        Invoice__ as Invoice,
        Payment_Method,
        Shift,
        initcap(trim(Lead_Source)) as Lead_Source,
        Notes,
    from source
    where kan is not null
)

select * from renamed