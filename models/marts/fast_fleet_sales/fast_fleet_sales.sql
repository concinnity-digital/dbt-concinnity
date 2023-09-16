{{
    config(
        materialized = 'view'
    )
}}

with unioned as (
select 
    date,
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
    date,
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
    date,
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
    date,
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
    date,
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
    date,
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
    date,
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
    date,
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
    date,
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
)

select * from unioned
