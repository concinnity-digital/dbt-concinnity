with 

source as (

    select * from {{ source('src_jan_pro_detroit_googleads', 'ads_SearchQueryStats_8432795746') }}
    union all 
    select * from {{ source('src_jan_pro_chicago_googleads', 'ads_SearchQueryStats_8579842656') }}
    union all 
    select * from {{ source('src_jan_pro_cincinnati_googleads', 'ads_SearchQueryStats_9075039811') }}
    union all 
    select * from {{ source('src_jan_pro_east_googleads', 'ads_SearchQueryStats_6611074754') }}
    union all 
    select * from {{ source('src_jan_pro_houston_googleads', 'ads_SearchQueryStats_3789896045') }}
    union all 
    select * from {{ source('src_jan_pro_massachusetts_googleads', 'ads_SearchQueryStats_4308472741') }}
    union all 
    select * from {{ source('src_jan_pro_nashville_googleads', 'ads_SearchQueryStats_7579569593') }}
    union all 
    select * from {{ source('src_jan_pro_north_newjersy_googleads', 'ads_SearchQueryStats_5011305995') }}
    union all 
    select * from {{ source('src_jan_pro_northeast_wisconsin_googleads', 'ads_SearchQueryStats_6640275400') }}
    union all 
    select * from {{ source('src_jan_pro_raleigh_googleads', 'ads_SearchQueryStats_9943921311') }}
    union all 
    select * from {{ source('src_jan_pro_triad_googleads', 'ads_SearchQueryStats_6782279639') }}
    union all 
    select * from {{ source('src_jan_pro_utah_googleads', 'ads_SearchQueryStats_9732427360') }}
    union all 
    select * from {{ source('src_jan_pro_capital_district_googleads', 'ads_SearchQueryStats_2074361282') }}
    union all 
    select * from {{ source('src_jan_pro_se_la_googleads', 'ads_SearchQueryStats_3025679247') }}
    union all 
    select * from {{ source('src_jan_pro_central_ga_googleads', 'ads_SearchQueryStats_7181072284') }}
    union all 
    select * from {{ source('src_jan_pro_twin_cities_googleads', 'ads_SearchQueryStats_3696124117') }}
    union all 
    select * from {{ source('src_jan_pro_new_england_googleads', 'ads_SearchQueryStats_1623951494') }}
    union all 
    select * from {{ source('src_jan_pro_west_michigan_googleads', 'ads_SearchQueryStats_3723961690') }}
    union all 
    select * from {{ source('src_jan_pro_sacramento_googleads', 'ads_SearchQueryStats_9810903396') }}
    union all 
    select * from {{ source('src_jan_pro_philadelphia_googleads', 'ads_SearchQueryStats_2550005947') }}
    union all 
    select * from {{ source('src_jan_pro_pittsburgh_googleads', 'ads_SearchQueryStats_4453396546') }}
    union all 
    select * from {{ source('src_jan_pro_jacksonville_googleads', 'ads_SearchQueryStats_5708690078') }}
    union all 
    select * from {{ source('src_jan_pro_georgia_aiken_googleads', 'ads_SearchQueryStats_9991303300') }}

),

renamed as (

    select
        ad_group_ad_ad_id,
        ad_group_id,
        campaign_id,
        customer_id,
        metrics_absolute_top_impression_percentage,
        metrics_all_conversions,
        metrics_all_conversions_from_interactions_rate,
        metrics_all_conversions_value,
        metrics_average_cost,
        metrics_average_cpc,
        metrics_average_cpe,
        metrics_average_cpm,
        metrics_average_cpv,
        metrics_clicks,
        metrics_conversions,
        metrics_conversions_from_interactions_rate,
        metrics_conversions_value,
        metrics_cost_micros,
        metrics_cost_per_all_conversions,
        metrics_cost_per_conversion,
        metrics_cross_device_conversions,
        metrics_ctr,
        metrics_engagement_rate,
        metrics_engagements,
        metrics_impressions,
        metrics_interaction_event_types,
        metrics_interaction_rate,
        metrics_interactions,
        metrics_top_impression_percentage,
        metrics_value_per_all_conversions,
        metrics_value_per_conversion,
        metrics_video_quartile_p100_rate,
        metrics_video_quartile_p25_rate,
        metrics_video_quartile_p50_rate,
        metrics_video_quartile_p75_rate,
        metrics_video_view_rate,
        metrics_video_views,
        metrics_view_through_conversions,
        search_term_view_search_term,
        search_term_view_status,
        segments_ad_network_type,
        segments_date,
        segments_day_of_week,
        segments_device,
        segments_keyword_ad_group_criterion,
        segments_month,
        segments_quarter,
        segments_search_term_match_type,
        segments_week,
        segments_year,
        _latest_date,
        _data_date

    from source

)

select * from renamed
