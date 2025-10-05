-- Query to analyze account A-fd7ad3 in stg__crm_accounts
select 
  'stg__crm_accounts' as source_table,
  *
from {{ ref('stg__crm_accounts') }} 
where account_id = 'A-fd7ad3'

union all

-- Query to analyze account A-fd7ad3 in stg__crm_churn_events  
select 
  'stg__crm_churn_events' as source_table,
  account_id,
  cast(churn_date as string) as account_name,
  cast(reason_code as string) as industry,
  cast(refund_amount_usd as string) as country,
  cast(preceding_upgrade_flag as string) as signup_date,
  cast(preceding_downgrade_flag as string) as referral_source,
  cast(is_reactivation as string) as plan_tier,
  cast(null as string) as seats,
  cast(null as string) as is_trial,
  cast(null as bool) as churn_flag
from {{ ref('stg__crm_churn_events') }} 
where account_id = 'A-fd7ad3'
