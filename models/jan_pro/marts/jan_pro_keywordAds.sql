with Criterion as(
select
  ad_group_criterion_criterion_id,
  ad_group_criterion_keyword_text,
  ad_group_criterion_quality_info_quality_score
from {{ ref('stg_jan_pro_googleads__ads_Keyword') }}
group by 1,2,3
),
customers as (
SELECT 
  customer_id,
  customer_descriptive_name
FROM {{ ref('stg_jan_pro_googleads__ads_Customer') }}
group by 1,2
)

SELECT 
  keywordstats.*
  ,criterion.ad_group_criterion_keyword_text
  ,criterion.ad_group_criterion_quality_info_quality_score
  ,customers.customer_descriptive_name
FROM {{ ref('stg_jan_pro_googleads__ads_KeywordStats') }} as keywordstats
LEFT JOIN Criterion as criterion
  on criterion.ad_group_criterion_criterion_id = keywordstats.ad_group_criterion_criterion_id
LEFT JOIN customers 
  on customers.customer_id = keywordstats.customer_id