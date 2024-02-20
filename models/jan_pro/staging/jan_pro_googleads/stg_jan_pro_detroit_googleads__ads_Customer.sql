with 

source as (

    select * from {{ source('src_jan_pro_detroit_googleads', 'ads_Customer_8432795746') }}
    union all 
    select * from {{ source('src_jan_pro_chicago_googleads', 'ads_Customer_8579842656') }}
    union all 
    select * from {{ source('src_jan_pro_cincinnati_googleads', 'ads_Customer_9075039811') }}
    union all 
    select * from {{ source('src_jan_pro_east_googleads', 'ads_Customer_6611074754') }}
    union all 
    select * from {{ source('src_jan_pro_houston_googleads', 'ads_Customer_3789896045') }}
    union all 
    select * from {{ source('src_jan_pro_massachusetts_googleads', 'ads_Customer_4308472741') }}

),

renamed as (

    select
        customer_id,
        customer_auto_tagging_enabled,
        customer_currency_code,
        customer_descriptive_name,
        customer_manager,
        customer_test_account,
        customer_time_zone,
        _latest_date,
        _data_date

    from source

)

select * from renamed
