-- Analysis of account A-fd7ad3 in stg__crm_churn_events
select 
  'stg__crm_churn_events' as source_table,
  account_id,
  churn_date,
  reason_code,
  refund_amount_usd,
  preceding_upgrade_flag,
  preceding_downgrade_flag,
  is_reactivation
from {{ ref('stg__crm_churn_events') }}
where account_id = 'A-fd7ad3'
order by churn_date
