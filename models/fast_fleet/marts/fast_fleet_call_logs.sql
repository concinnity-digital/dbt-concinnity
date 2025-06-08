with source as (
    select * from {{ source("src_fast_fleet_call_logs","yr_2024") }}
    union all
    select * from {{ source("src_fast_fleet_call_logs","yr_2025") }}
),
renamed as (
    select
        date_started as Date,
        operator_name,
        disposition,
        subdisposition_1,
        direction,
        external_number,
        internal_number,
        recording_url,
    from source
    where date_started is not null
)

select * from renamed