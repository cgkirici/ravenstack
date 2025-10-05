-- -----------------------------------------------------------------------------
-- Purpose: Feature popularity comparison between retained and churned customers
-- Granularity: One row per feature per customer status (retained/churned)
-- -----------------------------------------------------------------------------

{{ config(
	tags=['retention', 'features']
) }}

with accounts as (

	select 
		account_id,
		case 
			when customer_churn_flag = true then 'churned'
			else 'retained'
		end as customer_status
	from {{ ref('stg__crm_accounts') }}

),

feature_usage as (

	select * from {{ ref('stg__crm_feature_usage') }}

),

subscriptions as (

	select * from {{ ref('stg__crm_subscriptions') }}

),

account_feature_usage as (

	select
		a.customer_status,
		fu.feature_name,
		s.account_id,
		count(distinct fu.usage_id) as usage_events,
		sum(fu.usage_count) as usage_count,
		sum(fu.usage_duration_secs) as usage_duration_secs,
		count(distinct date_trunc(fu.usage_date, month)) as months_with_usage,
		max(fu.is_beta_feature) as is_beta_feature
	from feature_usage fu
	inner join subscriptions s using (subscription_id)
	inner join accounts a using (account_id)
	group by 1, 2, 3

),

customer_base as (

	select
		customer_status,
		count(distinct account_id) as total_customers
	from accounts
	group by 1

),

feature_popularity as (

	select
		afu.customer_status,
		afu.feature_name,
		max(afu.is_beta_feature) as is_beta_feature,
		
		-- Raw metrics
		count(distinct afu.account_id) as users_count,
		sum(afu.usage_events) as total_usage_events,
		sum(afu.usage_duration_secs) as total_usage_duration_secs,
		avg(afu.months_with_usage) as avg_months_with_usage,
		
		-- Normalized metrics (0-1 scale for weighted calculation)
		-- 1. User penetration (users / total customers)
		count(distinct afu.account_id) * 1.0 / cb.total_customers as penetration_rate,
		
		-- 2. Usage intensity (average events per user, normalized by max)
		(sum(afu.usage_events) * 1.0 / count(distinct afu.account_id)) / 
			max(sum(afu.usage_events) * 1.0 / count(distinct afu.account_id)) over (partition by afu.customer_status) as usage_intensity_score,
		
		-- 3. Duration engagement (average minutes per user, normalized by max)
		(sum(afu.usage_duration_secs) / 60.0 / count(distinct afu.account_id)) / 
			max(sum(afu.usage_duration_secs) / 60.0 / count(distinct afu.account_id)) over (partition by afu.customer_status) as duration_engagement_score,
		
		-- 4. Frequency consistency (average months active, normalized by max)
		avg(afu.months_with_usage) / 
			max(avg(afu.months_with_usage)) over (partition by afu.customer_status) as frequency_consistency_score,
		
		-- Average usage duration per user for stickiness calculation
		sum(afu.usage_duration_secs) / 60.0 / count(distinct afu.account_id) as avg_usage_minutes_per_user
		
	from account_feature_usage afu
	inner join customer_base cb using (customer_status)
	group by 1, 2, cb.total_customers

),

stickiness_components as (

	select
		feature_name,
		max(is_beta_feature) as is_beta_feature,
		
		-- Metrics by customer status
		max(case when customer_status = 'retained' then users_count else 0 end) as retained_users,
		max(case when customer_status = 'churned' then users_count else 0 end) as churned_users,
		max(case when customer_status = 'retained' then penetration_rate * 100 else 0 end) as retained_penetration_pct,
		max(case when customer_status = 'churned' then penetration_rate * 100 else 0 end) as churned_penetration_pct,
		max(case when customer_status = 'retained' then avg_usage_minutes_per_user else 0 end) as retained_avg_usage_minutes,
		max(case when customer_status = 'churned' then avg_usage_minutes_per_user else 0 end) as churned_avg_usage_minutes,
		
		-- Total users across both groups
		sum(users_count) as total_feature_users,
		
		-- Stickiness components
		-- 1. Adoption Gap (40%): % retained using - % churned using
		(max(case when customer_status = 'retained' then penetration_rate * 100 else 0 end) - 
		 max(case when customer_status = 'churned' then penetration_rate * 100 else 0 end)) as adoption_gap,
		
		-- 2. Usage Intensity Gap (30%): retained avg usage - churned avg usage
		(max(case when customer_status = 'retained' then avg_usage_minutes_per_user else 0 end) - 
		 max(case when customer_status = 'churned' then avg_usage_minutes_per_user else 0 end)) as usage_intensity_gap,
		
		-- 3. Retention Rate (30%): % of feature users who are retained
		max(case when customer_status = 'retained' then users_count else 0 end) * 100.0 / 
		nullif(sum(users_count), 0) as retention_rate
		
	from feature_popularity
	group by 1

),

stickiness_normalized as (

	select
		*,
		
		-- Normalize components to 0-100 scale for final calculation
		-- Adoption gap: normalize by max absolute value
		adoption_gap / nullif(greatest(
			max(abs(adoption_gap)) over(),
			1.0  -- minimum denominator to avoid extreme values
		), 0) * 100 as adoption_gap_normalized,
		
		-- Usage intensity gap: normalize by max absolute value  
		usage_intensity_gap / nullif(greatest(
			max(abs(usage_intensity_gap)) over(),
			1.0  -- minimum denominator to avoid extreme values
		), 0) * 100 as usage_intensity_gap_normalized,
		
		-- Retention rate is already 0-100, no normalization needed
		retention_rate as retention_rate_normalized
		
	from stickiness_components

),

final as (

	select
		{{ dbt_utils.generate_surrogate_key(['fp.customer_status', 'fp.feature_name']) }} as retention_metrics_key,
		fp.customer_status,
		fp.feature_name,
		fp.is_beta_feature,
		
		-- Raw metrics for BI visualization
		fp.users_count,
		round(fp.penetration_rate * 100, 2) as penetration_pct,
		fp.total_usage_events,
		round(fp.total_usage_duration_secs / 60.0, 2) as total_usage_minutes,
		round(fp.avg_months_with_usage, 2) as avg_months_with_usage,
		
		-- Individual component scores (0-1 scale)
		round(fp.penetration_rate, 4) as penetration_score,
		round(fp.usage_intensity_score, 4) as usage_intensity_score,
		round(fp.duration_engagement_score, 4) as duration_engagement_score,
		round(fp.frequency_consistency_score, 4) as frequency_consistency_score,
		
		-- Weighted popularity score (equal weight to all 4 components)
		round(
			(fp.penetration_rate + 
			 fp.usage_intensity_score + 
			 fp.duration_engagement_score + 
			 fp.frequency_consistency_score) / 4.0,
			4
		) as popularity_score,
		
		-- Popularity category for easy filtering/grouping
		case
			when (fp.penetration_rate + fp.usage_intensity_score + fp.duration_engagement_score + fp.frequency_consistency_score) / 4.0 >= 0.75 
				then 'highly_popular'
			when (fp.penetration_rate + fp.usage_intensity_score + fp.duration_engagement_score + fp.frequency_consistency_score) / 4.0 >= 0.5
				then 'moderately_popular'
			when (fp.penetration_rate + fp.usage_intensity_score + fp.duration_engagement_score + fp.frequency_consistency_score) / 4.0 >= 0.25
				then 'somewhat_popular'
			else 'low_popularity'
		end as popularity_category,
		
		-- Stickiness components (same values for both retained/churned rows)
		round(sn.adoption_gap, 2) as adoption_gap,
		round(sn.usage_intensity_gap, 2) as usage_intensity_gap,
		round(sn.retention_rate, 2) as retention_rate,
		
		-- Stickiness Score: (Adoption Gap × 40%) + (Usage Intensity Gap × 30%) + (Retention Rate × 30%)
		round(
			(sn.adoption_gap_normalized * 0.4) + 
			(sn.usage_intensity_gap_normalized * 0.3) + 
			(sn.retention_rate_normalized * 0.3),
			2
		) as stickiness_score,
		
		-- Stickiness category
		case
			when (sn.adoption_gap_normalized * 0.4) + (sn.usage_intensity_gap_normalized * 0.3) + (sn.retention_rate_normalized * 0.3) >= 75
				then 'highly_sticky'
			when (sn.adoption_gap_normalized * 0.4) + (sn.usage_intensity_gap_normalized * 0.3) + (sn.retention_rate_normalized * 0.3) >= 50
				then 'moderately_sticky'
			when (sn.adoption_gap_normalized * 0.4) + (sn.usage_intensity_gap_normalized * 0.3) + (sn.retention_rate_normalized * 0.3) >= 25
				then 'somewhat_sticky'
			else 'low_stickiness'
		end as stickiness_category

	from feature_popularity fp
	inner join stickiness_normalized sn using (feature_name)

)

select *
from final
order by customer_status, popularity_score desc, feature_name
