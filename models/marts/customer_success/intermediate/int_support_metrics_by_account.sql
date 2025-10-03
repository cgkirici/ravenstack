-- -----------------------------------------------------------------------------
-- Purpose: Account-level support ticket metrics and aggregations
-- Granularity: One row per account
-- -----------------------------------------------------------------------------

{{ config(
    materialized='view'
) }}

with 

-- Staging model references
support_tickets as (

	select * from {{ ref('stg__crm_support_tickets') }}

),

final as (

	select
		t.account_id,
		
		-- Ticket volume metrics
		count(distinct t.ticket_id) as total_tickets,
		min(t.submitted_at) as first_ticket_date,
		max(t.submitted_at) as last_ticket_date,
		
		-- Satisfaction metrics
		count(distinct case when t.satisfaction_score is not null then t.ticket_id end) as tickets_with_satisfaction,
		avg(case when t.satisfaction_score is not null then t.satisfaction_score end) as avg_satisfaction_score,
		min(case when t.satisfaction_score is not null then t.satisfaction_score end) as min_satisfaction_score,
		
		-- Low satisfaction analysis
		count(distinct case when t.satisfaction_score <= 3 then t.ticket_id end) as low_satisfaction_count,
		count(distinct case when t.satisfaction_score <= 2 then t.ticket_id end) as very_low_satisfaction_count,
		
		-- Last 30/60/90 days satisfaction (before churn or current)
		avg(case 
			when t.satisfaction_score is not null 
				and date_diff(current_date(), date(t.submitted_at), day) <= 30 
			then t.satisfaction_score 
		end) as satisfaction_last_30d,
		
		avg(case 
			when t.satisfaction_score is not null 
				and date_diff(current_date(), date(t.submitted_at), day) <= 90 
			then t.satisfaction_score 
		end) as satisfaction_last_90d,
		
		-- Response and resolution metrics
		avg(t.first_response_time_minutes) as avg_first_response_minutes,
		max(t.first_response_time_minutes) as max_first_response_minutes,
		avg(t.resolution_time_hours) as avg_resolution_hours,
		max(t.resolution_time_hours) as max_resolution_hours,
		
		-- Escalations
		count(distinct case when t.escalation_flag then t.ticket_id end) as escalated_tickets,
		
		-- Priority breakdown
		count(distinct case when t.priority = 'high' then t.ticket_id end) as high_priority_tickets,
		
		-- Recent ticket activity
		count(distinct case 
			when date_diff(current_date(), date(t.submitted_at), day) <= 30 
			then t.ticket_id 
		end) as tickets_last_30d,
		
		count(distinct case 
			when date_diff(current_date(), date(t.submitted_at), day) <= 90 
			then t.ticket_id 
		end) as tickets_last_90d
		
	from support_tickets t
	group by 1

)

select * from final
