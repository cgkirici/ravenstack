-- -----------------------------------------------------------------------------
-- Purpose: Conversion rate analysis for trial-to-paid and tier upgrade metrics
-- Granularity: One row per month per referral source + overall metrics row
-- Structure: Union of referral source-level metrics and overall aggregated metrics
-- -----------------------------------------------------------------------------

{{ config(
	tags=['conversion', 'revenue']
) }}

with subscriptions as (

	select * from {{ ref('stg__crm_subscriptions') }}

),

accounts as (

	select * from {{ ref('stg__crm_accounts') }}

),

months as (

	{{ generate_month_spine('2023-01-01', '2024-12-31') }}

),

trial_conversions_with_source as (

	select
		date_trunc(s.start_date, month) as conversion_month,
		a.referral_source,
		s.account_id,
		min(case when s.is_trial then s.start_date end) as trial_start_date,
		min(case when not s.is_trial then s.start_date end) as paid_start_date,
		min(case when not s.is_trial then s.plan_tier end) as first_paid_tier
	from subscriptions s
	inner join accounts a using (account_id)
	group by 1, 2, 3
	having trial_start_date is not null

),

tier_upgrades_with_source as (

	select
		date_trunc(s.start_date, month) as upgrade_month,
		a.referral_source,
		s.account_id,
		lag(s.plan_tier) over (partition by s.account_id order by s.start_date) as previous_tier,
		s.plan_tier as current_tier,
		case 
			when lag(s.plan_tier) over (partition by s.account_id order by s.start_date) = 'Basic' 
				and s.plan_tier in ('Pro', 'Enterprise') then true
			when lag(s.plan_tier) over (partition by s.account_id order by s.start_date) = 'Pro' 
				and s.plan_tier = 'Enterprise' then true
			else false
		end as is_upgrade
	from subscriptions s
	inner join accounts a using (account_id)
	where not s.is_trial
		and s.upgrade_flag = true

),

referral_source_metrics as (

	select
		m.month_start,
		tc.referral_source as overall_or_referral_source,
		count(distinct tc.account_id) as trial_starts,
		count(distinct case when tc.paid_start_date is not null then tc.account_id end) as trial_conversions,
		case 
			when count(distinct tc.account_id) > 0 
			then round(count(distinct case when tc.paid_start_date is not null then tc.account_id end) * 1.0 / 
				count(distinct tc.account_id), 4)
			else null
		end as trial_to_paid_conversion_rate,
		
		count(distinct case when tu.previous_tier = 'Basic' then tu.account_id end) as basic_tier_accounts,
		count(distinct case when tu.previous_tier = 'Pro' then tu.account_id end) as pro_tier_accounts,
		count(distinct case when tu.is_upgrade and tu.previous_tier = 'Basic' then tu.account_id end) as basic_to_higher_upgrades,
		count(distinct case when tu.is_upgrade and tu.previous_tier = 'Pro' then tu.account_id end) as pro_to_enterprise_upgrades,
		count(distinct case when tu.is_upgrade and tu.previous_tier = 'Basic' then tu.account_id end) + 
		count(distinct case when tu.is_upgrade and tu.previous_tier = 'Pro' then tu.account_id end) as total_tier_upgrades,
		case 
			when (count(distinct case when tu.previous_tier = 'Basic' then tu.account_id end) + 
				  count(distinct case when tu.previous_tier = 'Pro' then tu.account_id end)) > 0 
			then round((count(distinct case when tu.is_upgrade and tu.previous_tier = 'Basic' then tu.account_id end) + 
					   count(distinct case when tu.is_upgrade and tu.previous_tier = 'Pro' then tu.account_id end)) * 1.0 / 
					  (count(distinct case when tu.previous_tier = 'Basic' then tu.account_id end) + 
					   count(distinct case when tu.previous_tier = 'Pro' then tu.account_id end)), 4)
			else null
		end as lower_to_higher_tier_conversion_rate

	from months m
	cross join (select distinct referral_source from accounts where referral_source is not null) sources
	left join trial_conversions_with_source tc 
		on m.month_start = tc.conversion_month 
		and sources.referral_source = tc.referral_source
	left join tier_upgrades_with_source tu 
		on m.month_start = tu.upgrade_month 
		and sources.referral_source = tu.referral_source
	where m.month_start <= current_date()
	group by 1, 2

),

overall_metrics as (

	select
		m.month_start,
		'Overall' as overall_or_referral_source,
		count(distinct tc.account_id) as trial_starts,
		count(distinct case when tc.paid_start_date is not null then tc.account_id end) as trial_conversions,
		case 
			when count(distinct tc.account_id) > 0 
			then round(count(distinct case when tc.paid_start_date is not null then tc.account_id end) * 1.0 / 
				count(distinct tc.account_id), 4)
			else null
		end as trial_to_paid_conversion_rate,
		
		count(distinct case when tu.previous_tier = 'Basic' then tu.account_id end) as basic_tier_accounts,
		count(distinct case when tu.previous_tier = 'Pro' then tu.account_id end) as pro_tier_accounts,
		count(distinct case when tu.is_upgrade and tu.previous_tier = 'Basic' then tu.account_id end) as basic_to_higher_upgrades,
		count(distinct case when tu.is_upgrade and tu.previous_tier = 'Pro' then tu.account_id end) as pro_to_enterprise_upgrades,
		count(distinct case when tu.is_upgrade and tu.previous_tier = 'Basic' then tu.account_id end) + 
		count(distinct case when tu.is_upgrade and tu.previous_tier = 'Pro' then tu.account_id end) as total_tier_upgrades,
		case 
			when (count(distinct case when tu.previous_tier = 'Basic' then tu.account_id end) + 
				  count(distinct case when tu.previous_tier = 'Pro' then tu.account_id end)) > 0 
			then round((count(distinct case when tu.is_upgrade and tu.previous_tier = 'Basic' then tu.account_id end) + 
					   count(distinct case when tu.is_upgrade and tu.previous_tier = 'Pro' then tu.account_id end)) * 1.0 / 
					  (count(distinct case when tu.previous_tier = 'Basic' then tu.account_id end) + 
					   count(distinct case when tu.previous_tier = 'Pro' then tu.account_id end)), 4)
			else null
		end as lower_to_higher_tier_conversion_rate

	from months m
	left join trial_conversions_with_source tc on m.month_start = tc.conversion_month
	left join tier_upgrades_with_source tu on m.month_start = tu.upgrade_month
	where m.month_start <= current_date()
	group by 1, 2

),

final as (

	select * from referral_source_metrics
	where trial_starts > 0 or basic_tier_accounts > 0 or pro_tier_accounts > 0
	
	union all
	
	select * from overall_metrics
	where trial_starts > 0 or basic_tier_accounts > 0 or pro_tier_accounts > 0

)

select * from final
