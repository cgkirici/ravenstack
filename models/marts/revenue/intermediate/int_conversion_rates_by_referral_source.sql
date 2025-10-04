-- -----------------------------------------------------------------------------
-- Purpose: Conversion rates segmented by referral source
-- Granularity: One row per month per referral source
-- -----------------------------------------------------------------------------

{{ config(
	tags=['conversion', 'segmentation', 'intermediate']
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

trial_conversions_by_source as (

	select
		date_trunc(s.start_date, month) as conversion_month,
		a.referral_source,
		s.account_id,
		min(case when s.is_trial then s.start_date end) as trial_start_date,
		min(case when not s.is_trial then s.start_date end) as paid_start_date
	from subscriptions s
	inner join accounts a using (account_id)
	group by 1, 2, 3
	having trial_start_date is not null

),

tier_upgrades_by_source as (

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

monthly_trial_metrics_by_source as (

	select
		m.month_start,
		coalesce(tc.referral_source, 'Unknown') as referral_source,
		count(distinct tc.account_id) as trial_starts,
		count(distinct case when tc.paid_start_date is not null then tc.account_id end) as trial_conversions,
		case 
			when count(distinct tc.account_id) > 0 
			then round(count(distinct case when tc.paid_start_date is not null then tc.account_id end) * 1.0 / 
				count(distinct tc.account_id), 4)
			else null
		end as trial_to_paid_conversion_rate
	from months m
	left join trial_conversions_by_source tc
		on m.month_start = tc.conversion_month
	where tc.referral_source is not null or m.month_start <= current_date()
	group by 1, 2

),

monthly_upgrade_metrics_by_source as (

	select
		m.month_start,
		coalesce(tu.referral_source, 'Unknown') as referral_source,
		count(distinct case when tu.previous_tier in ('Basic', 'Pro') then tu.account_id end) as lower_tier_accounts,
		count(distinct case when tu.is_upgrade then tu.account_id end) as tier_upgrades,
		case 
			when count(distinct case when tu.previous_tier in ('Basic', 'Pro') then tu.account_id end) > 0 
			then round(count(distinct case when tu.is_upgrade then tu.account_id end) * 1.0 / 
				count(distinct case when tu.previous_tier in ('Basic', 'Pro') then tu.account_id end), 4)
			else null
		end as lower_to_higher_tier_conversion_rate
	from months m
	left join tier_upgrades_by_source tu
		on m.month_start = tu.upgrade_month
	where tu.referral_source is not null or m.month_start <= current_date()
	group by 1, 2

),

final as (

	select
		m.month_start,
		coalesce(tm.referral_source, um.referral_source, 'Unknown') as referral_source,
		coalesce(tm.trial_starts, 0) as trial_starts,
		coalesce(tm.trial_conversions, 0) as trial_conversions,
		tm.trial_to_paid_conversion_rate,
		coalesce(um.lower_tier_accounts, 0) as lower_tier_accounts,
		coalesce(um.tier_upgrades, 0) as tier_upgrades,
		um.lower_to_higher_tier_conversion_rate
	from months m
	full outer join monthly_trial_metrics_by_source tm using (month_start)
	full outer join monthly_upgrade_metrics_by_source um using (month_start, referral_source)
	where m.month_start <= current_date()
		and coalesce(tm.referral_source, um.referral_source) is not null

)

select * from final
