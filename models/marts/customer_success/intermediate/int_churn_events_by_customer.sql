-- -----------------------------------------------------------------------------
-- Purpose: All accounts that have churn events with their most recent churn date
-- Granularity: One row per churned account
-- -----------------------------------------------------------------------------

{{ config(
    materialized='table'
) }}

with 

-- Staging model references
accounts as (

	select * from {{ ref('stg__crm_accounts') }}

),

churn_events as (

	select * from {{ ref('stg__crm_churn_events') }}

),

-- Get the most recent churn date for each account
latest_churn_events as (

	select
		account_id,
		max(churn_date) as last_churn_event_date,
		-- Get the most recent churn details using window functions
		array_agg(churn_reason order by churn_date desc limit 1)[offset(0)] as latest_churn_reason,
		sum(refund_amount_usd) as total_refund_amount,
		array_agg(preceding_upgrade_flag order by churn_date desc limit 1)[offset(0)] as latest_preceding_upgrade_flag,
		array_agg(preceding_downgrade_flag order by churn_date desc limit 1)[offset(0)] as latest_preceding_downgrade_flag,
		array_agg(is_reactivation order by churn_date desc limit 1)[offset(0)] as latest_is_reactivation,
		count(*) as total_churn_events
		
	from churn_events
	group by 1

),

final as (

	select
		-- Account identifiers
		a.account_id,
		a.account_name,
		
		-- Account attributes
		a.industry,
		a.country,
		a.signup_date,
		a.referral_source,
		a.plan_tier,
		a.seats,
		a.is_trial,
		
		-- Churn information
		a.customer_churn_flag,
		case when a.customer_churn_flag is true then lce.last_churn_event_date else null end as customer_churn_date,
		lce.last_churn_event_date,
		lce.latest_churn_reason,
		lce.total_refund_amount,
		lce.latest_preceding_upgrade_flag,
		lce.latest_preceding_downgrade_flag,
		lce.latest_is_reactivation,
		lce.total_churn_events
		
	from accounts a
	inner join latest_churn_events lce on a.account_id = lce.account_id

)

select * from final
