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
    union all 
    select * from {{ source('src_jan_pro_nashville_googleads', 'ads_Customer_7579569593') }}
    union all 
    select * from {{ source('src_jan_pro_north_newjersy_googleads', 'ads_Customer_5011305995') }}
    union all 
    select * from {{ source('src_jan_pro_northeast_wisconsin_googleads', 'ads_Customer_6640275400') }}
    union all 
    select * from {{ source('src_jan_pro_raleigh_googleads', 'ads_Customer_9943921311') }}
    union all 
    select * from {{ source('src_jan_pro_triad_googleads', 'ads_Customer_6782279639') }}
    union all 
    select * from {{ source('src_jan_pro_utah_googleads', 'ads_Customer_9732427360') }}
    union all 
    select * from {{ source('src_jan_pro_capital_district_googleads', 'ads_Customer_2074361282') }}
    union all 
    select * from {{ source('src_jan_pro_se_la_googleads', 'ads_Customer_3025679247') }}
    union all 
    select * from {{ source('src_jan_pro_central_ga_googleads', 'ads_Customer_7181072284') }}
    union all 
    select * from {{ source('src_jan_pro_twin_cities_googleads', 'ads_Customer_3696124117') }}
    union all 
    select * from {{ source('src_jan_pro_new_england_googleads', 'ads_Customer_1623951494') }}
    union all 
    select * from {{ source('src_jan_pro_west_michigan_googleads', 'ads_Customer_3723961690') }}
    union all 
    select * from {{ source('src_jan_pro_sacramento_googleads', 'ads_Customer_9810903396') }}
    union all 
    select * from {{ source('src_jan_pro_philadelphia_googleads', 'ads_Customer_2550005947') }}
    union all 
    select * from {{ source('src_jan_pro_pittsburgh_googleads', 'ads_Customer_4453396546') }}
    union all 
    select * from {{ source('src_jan_pro_jacksonville_googleads', 'ads_Customer_5708690078') }}
    union all 
    select * from {{ source('src_jan_pro_georgia_aiken_googleads', 'ads_Customer_9991303300') }}
    
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
