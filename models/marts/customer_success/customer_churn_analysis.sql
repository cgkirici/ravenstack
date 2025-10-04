-- -----------------------------------------------------------------------------
-- Purpose: Customer-level analysis with all churn factors
-- Granularity: One row per account
-- -----------------------------------------------------------------------------

{{ config(
    materialized='table',
    cluster_by=['customer_churn_flag', 'plan_tier', 'country']
) }}

with accounts as (

	select * from {{ ref('int_churn_events_by_customer') }}

),

subscription_details as (

	select * from {{ ref('account_subscription_details') }}

),

support_metrics as (

	select * from {{ ref('int_support_metrics_by_account') }}

),

final as (

	select
		-- Account identifiers
		ab.account_id,
		ab.account_name,
		
		-- Account attributes
		ab.industry,
		ab.country,
		ab.signup_date,
		ab.referral_source,
		ab.plan_tier,
		ab.seats,
		ab.is_trial,
		
		-- Churn status
		ab.customer_churn_flag,
		ab.customer_churn_date,
		ab.latest_churn_reason,
		ab.total_refund_amount,
		
		-- Subscription history
		coalesce(iasd.total_subscriptions, 0) as total_subscriptions,
		iasd.upgrade_count,
		iasd.downgrade_count,
		
		-- Support ticket metrics
		coalesce(sm.total_tickets, 0) as total_tickets,
		
		-- Satisfaction score
		round(sm.avg_satisfaction_score, 2) as avg_satisfaction_score,
		
		-- Flags for correlation analysis
		case when sm.avg_satisfaction_score <= 3 then true else false end as has_low_satisfaction,
		case when sm.escalated_tickets > 0 then true else false end as has_escalations,
		case when sm.tickets_last_90d >= 5 then true else false end as high_ticket_volume
		
	from accounts ab
	left join support_metrics sm on ab.account_id = sm.account_id
	left join subscription_details iasd on ab.account_id = iasd.account_id
	order by 
		ab.customer_churn_flag desc,
		sm.avg_satisfaction_score asc nulls last

)

select * from final
