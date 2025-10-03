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
		sm.first_ticket_date,
		sm.last_ticket_date,
		
		-- Satisfaction scores
		round(sm.avg_satisfaction_score, 2) as avg_satisfaction_score,
		sm.min_satisfaction_score,
		round(sm.satisfaction_last_30d, 2) as satisfaction_last_30d,
		round(sm.satisfaction_last_90d, 2) as satisfaction_last_90d,
		
		-- Low satisfaction metrics
		coalesce(sm.low_satisfaction_count, 0) as low_satisfaction_count,
		coalesce(sm.very_low_satisfaction_count, 0) as very_low_satisfaction_count,
		round(safe_divide(sm.low_satisfaction_count * 100.0, nullif(sm.tickets_with_satisfaction, 0)), 2) as pct_low_satisfaction,
		
		-- Response metrics
		round(sm.avg_first_response_minutes, 1) as avg_first_response_minutes,
		sm.max_first_response_minutes,
		round(sm.avg_resolution_hours, 1) as avg_resolution_hours,
		sm.max_resolution_hours,
		
		-- Escalations
		coalesce(sm.escalated_tickets, 0) as escalated_tickets,
		round(safe_divide(sm.escalated_tickets * 100.0, nullif(sm.total_tickets, 0)), 2) as pct_tickets_escalated,
		
		-- Recent activity
		coalesce(sm.tickets_last_30d, 0) as tickets_last_30d,
		coalesce(sm.tickets_last_90d, 0) as tickets_last_90d,
		coalesce(sm.high_priority_tickets, 0) as high_priority_tickets,
		
		-- Risk scoring (0-100, higher = more at risk)
		case
			when ab.customer_churn_flag then null  -- Don't score already churned customers
			else (
				-- Low satisfaction component (0-40 points)
				coalesce(
					case 
						when sm.avg_satisfaction_score <= 2 then 40
						when sm.avg_satisfaction_score <= 3 then 30
						when sm.avg_satisfaction_score <= 3.5 then 20
						when sm.avg_satisfaction_score <= 4 then 10
						else 0
					end, 0
				) +
				-- Escalation component (0-20 points)
				coalesce(
					case 
						when safe_divide(sm.escalated_tickets, nullif(sm.total_tickets, 0)) >= 0.3 then 20
						when safe_divide(sm.escalated_tickets, nullif(sm.total_tickets, 0)) >= 0.2 then 15
						when safe_divide(sm.escalated_tickets, nullif(sm.total_tickets, 0)) >= 0.1 then 10
						else 0
					end, 0
				) +
				-- Recent low satisfaction component (0-25 points)
				coalesce(
					case 
						when sm.satisfaction_last_30d <= 2 then 25
						when sm.satisfaction_last_30d <= 3 then 15
						when sm.satisfaction_last_90d <= 3 then 10
						else 0
					end, 0
				) +
				-- High ticket volume component (0-15 points)
				coalesce(
					case 
						when sm.tickets_last_90d >= 10 then 15
						when sm.tickets_last_90d >= 5 then 10
						when sm.tickets_last_90d >= 3 then 5
						else 0
					end, 0
				)
			)
		end as churn_risk_score,
		
		-- Risk category
		case
			when ab.customer_churn_flag then 'Churned'
			when (
				coalesce(
					case 
						when sm.avg_satisfaction_score <= 2 then 40
						when sm.avg_satisfaction_score <= 3 then 30
						when sm.avg_satisfaction_score <= 3.5 then 20
						when sm.avg_satisfaction_score <= 4 then 10
						else 0
					end, 0
				) +
				coalesce(
					case 
						when safe_divide(sm.escalated_tickets, nullif(sm.total_tickets, 0)) >= 0.3 then 20
						when safe_divide(sm.escalated_tickets, nullif(sm.total_tickets, 0)) >= 0.2 then 15
						when safe_divide(sm.escalated_tickets, nullif(sm.total_tickets, 0)) >= 0.1 then 10
						else 0
					end, 0
				) +
				coalesce(
					case 
						when sm.satisfaction_last_30d <= 2 then 25
						when sm.satisfaction_last_30d <= 3 then 15
						when sm.satisfaction_last_90d <= 3 then 10
						else 0
					end, 0
				) +
				coalesce(
					case 
						when sm.tickets_last_90d >= 10 then 15
						when sm.tickets_last_90d >= 5 then 10
						when sm.tickets_last_90d >= 3 then 5
						else 0
					end, 0
				)
			) >= 60 then 'High Risk'
			when (
				coalesce(
					case 
						when sm.avg_satisfaction_score <= 2 then 40
						when sm.avg_satisfaction_score <= 3 then 30
						when sm.avg_satisfaction_score <= 3.5 then 20
						when sm.avg_satisfaction_score <= 4 then 10
						else 0
					end, 0
				) +
				coalesce(
					case 
						when safe_divide(sm.escalated_tickets, nullif(sm.total_tickets, 0)) >= 0.3 then 20
						when safe_divide(sm.escalated_tickets, nullif(sm.total_tickets, 0)) >= 0.2 then 15
						when safe_divide(sm.escalated_tickets, nullif(sm.total_tickets, 0)) >= 0.1 then 10
						else 0
					end, 0
				) +
				coalesce(
					case 
						when sm.satisfaction_last_30d <= 2 then 25
						when sm.satisfaction_last_30d <= 3 then 15
						when sm.satisfaction_last_90d <= 3 then 10
						else 0
					end, 0
				) +
				coalesce(
					case 
						when sm.tickets_last_90d >= 10 then 15
						when sm.tickets_last_90d >= 5 then 10
						when sm.tickets_last_90d >= 3 then 5
						else 0
					end, 0
				)
			) >= 40 then 'Medium Risk'
			when (
				coalesce(
					case 
						when sm.avg_satisfaction_score <= 2 then 40
						when sm.avg_satisfaction_score <= 3 then 30
						when sm.avg_satisfaction_score <= 3.5 then 20
						when sm.avg_satisfaction_score <= 4 then 10
						else 0
					end, 0
				) +
				coalesce(
					case 
						when safe_divide(sm.escalated_tickets, nullif(sm.total_tickets, 0)) >= 0.3 then 20
						when safe_divide(sm.escalated_tickets, nullif(sm.total_tickets, 0)) >= 0.2 then 15
						when safe_divide(sm.escalated_tickets, nullif(sm.total_tickets, 0)) >= 0.1 then 10
						else 0
					end, 0
				) +
				coalesce(
					case 
						when sm.satisfaction_last_30d <= 2 then 25
						when sm.satisfaction_last_30d <= 3 then 15
						when sm.satisfaction_last_90d <= 3 then 10
						else 0
					end, 0
				) +
				coalesce(
					case 
						when sm.tickets_last_90d >= 10 then 15
						when sm.tickets_last_90d >= 5 then 10
						when sm.tickets_last_90d >= 3 then 5
						else 0
					end, 0
				)
			) > 0 then 'Low Risk'
			else 'Healthy'
		end as risk_category,
		
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
