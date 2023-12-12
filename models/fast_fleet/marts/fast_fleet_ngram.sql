with Criterion as(
select
  CriterionId,
  Criteria
from {{ ref('stg_fast_fleet_googleads__keyword') }}
group by 1,2
),
customers as (
SELECT 
  ExternalCustomerID,
  CustomerDescriptiveName
FROM {{ ref('stg_fast_fleet_googleads__customer') }}
group by 1,2
)

SELECT 
  keywordstats.*
  ,criterion.Criteria
  -- ,REGEXP_EXTRACT_ALL(lower(criterion.Criteria), '[a-z]+') as keywords_array
  ,ML.NGRAMS(REGEXP_EXTRACT_ALL(lower(criterion.Criteria), '[a-z]+'), [1,3]) as ngrams
  ,customers.CustomerDescriptiveName
FROM {{ ref('stg_fast_fleet_googleads__keywordstats') }} as keywordstats
LEFT JOIN Criterion as criterion
  on criterion.CriterionId = keywordstats.CriterionId
LEFT JOIN customers 
  on customers.ExternalCustomerID = keywordstats.ExternalCustomerId
where customers.CustomerDescriptiveName = "Fast Fleet RS"