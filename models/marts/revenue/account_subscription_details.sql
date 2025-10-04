-- -----------------------------------------------------------------------------
-- Purpose: Account-level subscription details and history
-- Granularity: One row per account
-- -----------------------------------------------------------------------------

{{ config(
    materialized='view'
) }}

with accounts as (
	
	select * from {{ ref('stg__crm_accounts') }}

),

subscriptions as (

	select * from {{ ref('stg__crm_subscriptions') }}

),

final as (

	select
		a.account_id,
		a.account_name,
		a.customer_churn_flag,

		count(distinct s.subscription_id) as total_subscriptions,
		min(s.start_date) as first_subscription_date,
		max(s.start_date) as last_subscription_date,
	
		-- Current or last subscription details
		array_agg(s.plan_tier order by s.start_date desc limit 1)[offset(0)] as latest_plan_tier,
		array_agg(s.seats order by s.start_date desc limit 1)[offset(0)] as latest_seats,
		
		-- Upgrade/downgrade history
		sum(case when s.upgrade_flag then 1 else 0 end) as upgrade_count,
		sum(case when s.downgrade_flag then 1 else 0 end) as downgrade_count,
		
	from accounts a
	left join subscriptions s on a.account_id = s.account_id
	group by 1, 2, 3

)

select * from final
