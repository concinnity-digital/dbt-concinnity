{{
    config(
        unique_key="surrogate_key",
        partition_by={"field": "date", "data_type": "date"},
    )
}}

with
    region_mapping as (select * from {{ ref("stg_fast_fleet__region_mapping") }}),

    unioned as (
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            null as shift
        from {{ ref("stg_fast_fleet_sales__2022_01") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            null as shift
        from {{ ref("stg_fast_fleet_sales__2022_02") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            null as shift
        from {{ ref("stg_fast_fleet_sales__2022_03") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            null as shift
        from {{ ref("stg_fast_fleet_sales__2022_04") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            null as shift
        from {{ ref("stg_fast_fleet_sales__2022_05") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            null as shift
        from {{ ref("stg_fast_fleet_sales__2022_06") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            null as shift
        from {{ ref("stg_fast_fleet_sales__2022_07") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            null as shift
        from {{ ref("stg_fast_fleet_sales__2022_08") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            null as shift
        from {{ ref("stg_fast_fleet_sales__2022_09") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            null as shift
        from {{ ref("stg_fast_fleet_sales__2022_10") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            null as shift
        from {{ ref("stg_fast_fleet_sales__2022_11") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            null as shift
        from {{ ref("stg_fast_fleet_sales__2022_12") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2023_01") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2023_02") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2023_03") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2023_04") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2023_05") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2023_06") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2023_07") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2023_08") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2023_09") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2023_10") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2023_11") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2023_12") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2024_01") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2024_02") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2024_03") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2024_04") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2024_05") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2024_06") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2024_07") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2024_08") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2024_09") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2024_10") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2024_11") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2024_12") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2025_01") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2025_02") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2025_03") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2025_04") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2025_05") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2025_06") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2025_07") }}
        union all
        select
            date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift
        from {{ ref("stg_fast_fleet_sales__2025_09") }}
        
        
        

    ),
    add_row_num as (select *, row_number() over () as sales_rn from unioned),
    add_region as (
        select
            add_row_num.*,
            region_mapping.region,
            row_number() over (
                partition by add_row_num.sales_rn
                order by region_mapping.start_date desc
            ) as row_num
        from add_row_num
        left join
            region_mapping
            on region_mapping.truck = add_row_num.truck
            and add_row_num.date >= region_mapping.start_date
    ),
    final as (
        select
            safe_cast(date as Date) as date,
            customer,
            amount,
            labor_cost,
            job_supplies,
            gross_revenue,
            gp_margin,
            service_type,
            classification,
            job,
            payment_method,
            truck,
            lead_source,
            notes,
            shift,
            coalesce(region, 'No Region/Truck') as region
        from add_region
        where
            row_num = 1 or row_num is null

    )

select *
from final
