with 

source as (

    select * from {{ source('fastfleet_googleads', 'Customer_4201378109') }}

),

renamed as (

    select
        externalcustomerid,
        isautotaggingenabled,
        accountcurrencycode,
        accountdescriptivename,
        customerdescriptivename,
        canmanageclients,
        istestaccount,
        accounttimezone,
        _latest_date,
        _data_date

    from source

)

select * from source
