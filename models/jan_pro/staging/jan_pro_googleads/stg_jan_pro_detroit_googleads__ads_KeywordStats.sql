with 

source as (

    select * from {{ source('src_jan_pro_detroit_googleads', 'ads_KeywordStats_8432795746') }}
    union all 
    select * from {{ source('src_jan_pro_chicago_googleads', 'ads_KeywordStats_8579842656') }}
    union all 
    select * from {{ source('src_jan_pro_cincinnati_googleads', 'ads_KeywordStats_9075039811') }}

),

renamed as (

    select
        ad_group_criterion_criterion_id,
        ad_group_id,
        campaign_id,
        customer_id,
        ad_group_base_ad_group,
        campaign_base_campaign,
        metrics_active_view_cpm,
        metrics_active_view_ctr,
        metrics_active_view_impressions,
        metrics_active_view_measurability,
        metrics_active_view_measurable_cost_micros,
        metrics_active_view_measurable_impressions,
        metrics_active_view_viewability,
        metrics_average_cost,
        metrics_average_cpc,
        metrics_average_cpm,
        metrics_clicks,
        metrics_conversions,
        metrics_conversions_from_interactions_rate,
        metrics_conversions_value,
        metrics_cost_micros,
        metrics_cost_per_conversion,
        metrics_cost_per_current_model_attributed_conversion,
        metrics_ctr,
        metrics_current_model_attributed_conversions,
        metrics_current_model_attributed_conversions_value,
        metrics_gmail_forwards,
        metrics_gmail_saves,
        metrics_gmail_secondary_clicks,
        metrics_impressions,
        metrics_interaction_event_types,
        metrics_interaction_rate,
        metrics_interactions,
        metrics_value_per_conversion,
        metrics_value_per_current_model_attributed_conversion,
        segments_ad_network_type,
        segments_click_type,
        segments_date,
        segments_day_of_week,
        segments_device,
        segments_month,
        segments_quarter,
        segments_week,
        segments_year,
        _latest_date,
        _data_date

    from source

)

select * from renamed
