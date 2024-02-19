with customers as (
SELECT 
  customer_id,
  customer_descriptive_name
FROM {{ ref('stg_jan_pro_detroit_googleads__ads_Customer') }}
group by 1,2
)

SELECT
  search_terms.*,
  customers.customer_descriptive_name,
  -- ,REGEXP_EXTRACT_ALL(lower(criterion.Criteria), '[a-z]+') as keywords_array
  ML.NGRAMS(REGEXP_EXTRACT_ALL(lower(search_terms.search_term_view_search_term), '[a-z]+'), [1,5]) as ngrams
FROM {{ ref('stg_jan_pro_detroit_googleads__ads_SearchQueryStats') }} as search_terms
LEFT JOIN customers
  on customers.customer_id= search_terms.customer_id
