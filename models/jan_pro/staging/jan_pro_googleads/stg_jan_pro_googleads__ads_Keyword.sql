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
    union all 
    select * from {{ source('src_jan_pro_nashville_googleads', 'ads_Keyword_7579569593') }}
    union all 
    select * from {{ source('src_jan_pro_north_newjersy_googleads', 'ads_Keyword_5011305995') }}
    union all 
    select * from {{ source('src_jan_pro_northeast_wisconsin_googleads', 'ads_Keyword_6640275400') }}
    union all 
    select * from {{ source('src_jan_pro_raleigh_googleads', 'ads_Keyword_9943921311') }}
    union all 
    select * from {{ source('src_jan_pro_triad_googleads', 'ads_Keyword_6782279639') }}
    union all 
    select * from {{ source('src_jan_pro_utah_googleads', 'ads_Keyword_9732427360') }}
    union all 
    select * from {{ source('src_jan_pro_capital_district_googleads', 'ads_Keyword_2074361282') }}
    union all 
    select * from {{ source('src_jan_pro_se_la_googleads', 'ads_Keyword_3025679247') }}
    union all 
    select * from {{ source('src_jan_pro_central_ga_googleads', 'ads_Keyword_7181072284') }}
    union all 
    select * from {{ source('src_jan_pro_twin_cities_googleads', 'ads_Keyword_3696124117') }}
    union all 
    select * from {{ source('src_jan_pro_new_england_googleads', 'ads_Keyword_1623951494') }}
    union all 
    select * from {{ source('src_jan_pro_west_michigan_googleads', 'ads_Keyword_3723961690') }}
    union all 
    select * from {{ source('src_jan_pro_sacramento_googleads', 'ads_Keyword_9810903396') }}
    union all 
    select * from {{ source('src_jan_pro_philadelphia_googleads', 'ads_Keyword_2550005947') }}
    union all 
    select * from {{ source('src_jan_pro_pittsburgh_googleads', 'ads_Keyword_4453396546') }}
    union all 
    select * from {{ source('src_jan_pro_jacksonville_googleads', 'ads_Keyword_5708690078') }}
    union all 
    select * from {{ source('src_jan_pro_georgia_aiken_googleads', 'ads_Keyword_9991303300') }}

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
