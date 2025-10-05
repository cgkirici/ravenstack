-- Simple data quality check for account A-e7a1e2
select 
	'SUBSCRIPTION_COUNT' as metric,
	cast(count(*) as string) as value,
	'Total subscriptions for account A-e7a1e2' as description
from {{ ref('stg__crm_subscriptions') }}
where account_id = 'A-e7a1e2'

union all

select 
	'ENDED_SUBSCRIPTION_COUNT' as metric,
	cast(count(*) as string) as value,
	'Subscriptions with end_date for account A-e7a1e2' as description
from {{ ref('stg__crm_subscriptions') }}
where account_id = 'A-e7a1e2' and end_date is not null

union all

select 
	'CHURN_EVENT_COUNT' as metric,
	cast(count(*) as string) as value,
	'Churn events for account A-e7a1e2' as description
from {{ ref('stg__crm_churn_events') }}
where account_id = 'A-e7a1e2'

order by metric
