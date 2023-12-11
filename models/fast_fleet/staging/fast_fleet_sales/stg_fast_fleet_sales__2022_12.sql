with source as (
    select * from {{ source("src_fast_fleet_sales","2022_12") }}
),
renamed as (
    select
        safe_cast(Date as DATE) as Date,
        Customer,
        Amount,
        Labor as Labor_Cost,
        Job_Supplies,
        safe_cast(Gross_Revenue as FLOAT64) as Gross_Revenue,
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