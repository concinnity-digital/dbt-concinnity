with 

source as (

    select * from {{ source('analytics_425270834', 'events_20240411') }}

),

renamed as (

    select
        event_date,
        event_timestamp,
        event_name,
        event_params,
        event_previous_timestamp,
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_pseudo_id,
        privacy_info,
        user_properties,
        user_first_touch_timestamp,
        user_ltv,
        device,
        geo,
        app_info,
        traffic_source,
        stream_id,
        platform,
        event_dimensions,
        ecommerce,
        items,
        collected_traffic_source,
        is_active_user

    from source

)

select * from renamed
