{{
    config(
        unique_key = "surrogate_key",
        partition_by ={ "field": "date", "data_type": "date" }
    )
}}

with region_mapping as (
    select * from {{ ref('stg_fast_fleet__region_mapping') }}
),

unioned as (
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        null as Shift
    from {{ ref('stg_fast_fleet_sales__2022_10') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        null as Shift
    from {{ ref('stg_fast_fleet_sales__2022_11') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        null as Shift
    from {{ ref('stg_fast_fleet_sales__2022_12') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        Shift
    from {{ ref('stg_fast_fleet_sales__2023_01') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        Shift
    from {{ ref('stg_fast_fleet_sales__2023_02') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        Shift
    from {{ ref('stg_fast_fleet_sales__2023_03') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        Shift
    from {{ ref('stg_fast_fleet_sales__2023_04') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        Shift
    from {{ ref('stg_fast_fleet_sales__2023_05') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        Shift
    from {{ ref('stg_fast_fleet_sales__2023_06') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        Shift
    from {{ ref('stg_fast_fleet_sales__2023_07') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        Shift
    from {{ ref('stg_fast_fleet_sales__2023_08') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        Shift
    from {{ ref('stg_fast_fleet_sales__2023_09') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        Shift
    from {{ ref('stg_fast_fleet_sales__2023_10') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        Shift
    from {{ ref('stg_fast_fleet_sales__2023_11') }}
    union all
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,    
        Lead_Source,    
        Notes,
        Shift
    from {{ ref('stg_fast_fleet_sales__2023_12') }}
),
add_row_num as (
    select 
        *,
        row_number() over () as sales_rn
    from unioned
),
add_region as (
    select 
        add_row_num.*,
        region_mapping.Region,
        row_number() over (partition by add_row_num.sales_rn  order by region_mapping.Start_Date desc) as row_num
    from add_row_num
    left join region_mapping
        on region_mapping.truck = add_row_num.Truck
        and add_row_num.Date >= region_mapping.start_date
),
final as(
    select 
        Date,
        Customer,
        Amount,
        Labor_Cost,
        Job_Supplies,
        Gross_Revenue,
        GP_Margin,
        Service_Type,
        Classification,
        Job,
        Payment_Method,
        Truck,
        Lead_Source,
        Notes,
        Shift,
        coalesce(Region, 'No Region/Truck') as Region
from add_region
where row_num = 1 or row_num is null
)

select * from final
