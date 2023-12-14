with 

source as (

    select * from {{ source('jan_pro_googleads', 'ads_Customer_8432795746') }}

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
