-- Analysis of account A-fd7ad3 in stg__crm_subscriptions
select 
  'stg__crm_subscriptions' as source_table,
  account_id,
  subscription_id,
  plan_tier,
  seats,
  start_date,
  end_date,
  case when end_date is not null then 'ENDED' else 'ACTIVE' end as subscription_status,
  upgrade_flag,
  downgrade_flag
from {{ ref('stg__crm_subscriptions') }}
where account_id = 'A-fd7ad3'
order by start_date
