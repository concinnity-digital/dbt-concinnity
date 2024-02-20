with 

source as (

    select * from {{ source('src_jan_pro_detroit_googleads', 'ads_Keyword_8432795746') }}
    union all 
    select * from {{ source('src_jan_pro_chicago_googleads', 'ads_Keyword_8579842656') }}
    union all 
    select * from {{ source('src_jan_pro_cincinnati_googleads', 'ads_Keyword_9075039811') }}
    union all 
    select * from {{ source('src_jan_pro_east_googleads', 'ads_Keyword_6611074754') }}
    union all 
    select * from {{ source('src_jan_pro_houston_googleads', 'ads_Keyword_3789896045') }}
    union all 
    select * from {{ source('src_jan_pro_massachusetts_googleads', 'ads_Keyword_4308472741') }}

),

renamed as (

    select
        ad_group_criterion_criterion_id,
        ad_group_id,
        campaign_id,
        customer_id,
        ad_group_criterion_approval_status,
        ad_group_criterion_effective_cpc_bid_micros,
        ad_group_criterion_effective_cpc_bid_source,
        ad_group_criterion_effective_cpm_bid_micros,
        ad_group_criterion_final_mobile_urls,
        ad_group_criterion_final_url_suffix,
        ad_group_criterion_final_urls,
        ad_group_criterion_keyword_match_type,
        ad_group_criterion_keyword_text,
        ad_group_criterion_negative,
        ad_group_criterion_position_estimates_estimated_add_clicks_at_first_position_cpc,
        ad_group_criterion_position_estimates_estimated_add_cost_at_first_position_cpc,
        ad_group_criterion_position_estimates_first_page_cpc_micros,
        ad_group_criterion_position_estimates_first_position_cpc_micros,
        ad_group_criterion_position_estimates_top_of_page_cpc_micros,
        ad_group_criterion_quality_info_creative_quality_score,
        ad_group_criterion_quality_info_post_click_quality_score,
        ad_group_criterion_quality_info_quality_score,
        ad_group_criterion_quality_info_search_predicted_ctr,
        ad_group_criterion_status,
        ad_group_criterion_system_serving_status,
        ad_group_criterion_topic_topic_constant,
        ad_group_criterion_tracking_url_template,
        ad_group_criterion_url_custom_parameters,
        campaign_bidding_strategy,
        campaign_bidding_strategy_type,
        campaign_manual_cpc_enhanced_cpc_enabled,
        campaign_percent_cpc_enhanced_cpc_enabled,
        _latest_date,
        _data_date

    from source

)

select * from renamed
