-- -----------------------------------------------------------------------------
-- Purpose: Conversion rate analysis for trial-to-paid and tier upgrade metrics
-- Granularity: One row per month with conversion rates and supporting metrics
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

trial_conversions as (

	select
		date_trunc(s.start_date, month) as conversion_month,
		s.account_id,
		min(case when s.is_trial then s.start_date end) as trial_start_date,
		min(case when not s.is_trial then s.start_date end) as paid_start_date,
		min(case when not s.is_trial then s.plan_tier end) as first_paid_tier
	from subscriptions s
	group by 1, 2
	having trial_start_date is not null

),

tier_upgrades as (

	select
		date_trunc(s.start_date, month) as upgrade_month,
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
	where not s.is_trial
		and s.upgrade_flag = true

),

monthly_trial_metrics as (

	select
		m.month_start,
		count(distinct tc.account_id) as trial_starts,
		count(distinct case when tc.paid_start_date is not null then tc.account_id end) as trial_conversions
	from months m
	left join trial_conversions tc
		on m.month_start = tc.conversion_month
	group by 1

),

monthly_upgrade_metrics as (

	select
		m.month_start,
		count(distinct case when tu.previous_tier = 'Basic' then tu.account_id end) as basic_tier_accounts,
		count(distinct case when tu.previous_tier = 'Pro' then tu.account_id end) as pro_tier_accounts,
		count(distinct case when tu.is_upgrade and tu.previous_tier = 'Basic' then tu.account_id end) as basic_to_higher_upgrades,
		count(distinct case when tu.is_upgrade and tu.previous_tier = 'Pro' then tu.account_id end) as pro_to_enterprise_upgrades
	from months m
	left join tier_upgrades tu
		on m.month_start = tu.upgrade_month
	group by 1

),

conversion_by_source as (

	select * from {{ ref('int_conversion_rates_by_referral_source') }}

),

best_trial_conversion_source as (

	select
		month_start,
		referral_source as best_trial_conversion_source,
		trial_to_paid_conversion_rate as best_trial_conversion_rate
	from (
		select
			month_start,
			referral_source,
			trial_to_paid_conversion_rate,
			row_number() over (partition by month_start order by trial_to_paid_conversion_rate desc, trial_conversions desc) as rn
		from conversion_by_source
		where trial_to_paid_conversion_rate is not null
			and trial_starts > 0
	) ranked
	where rn = 1

),

best_tier_upgrade_source as (

	select
		month_start,
		referral_source as best_tier_upgrade_source,
		lower_to_higher_tier_conversion_rate as best_tier_upgrade_rate
	from (
		select
			month_start,
			referral_source,
			lower_to_higher_tier_conversion_rate,
			row_number() over (partition by month_start order by lower_to_higher_tier_conversion_rate desc, tier_upgrades desc) as rn
		from conversion_by_source
		where lower_to_higher_tier_conversion_rate is not null
			and lower_tier_accounts > 0
	) ranked
	where rn = 1

),

final as (

	select
		m.month_start,
		
		-- trial conversion metrics
		coalesce(tm.trial_starts, 0) as trial_starts,
		coalesce(tm.trial_conversions, 0) as trial_conversions,
		case 
			when coalesce(tm.trial_starts, 0) > 0 
			then round(coalesce(tm.trial_conversions, 0) * 100.0 / tm.trial_starts, 2)
			else null
		end as trial_to_paid_conversion_rate,
		
		-- tier upgrade metrics
		coalesce(um.basic_tier_accounts, 0) as basic_tier_accounts,
		coalesce(um.pro_tier_accounts, 0) as pro_tier_accounts,
		coalesce(um.basic_to_higher_upgrades, 0) as basic_to_higher_upgrades,
		coalesce(um.pro_to_enterprise_upgrades, 0) as pro_to_enterprise_upgrades,
		coalesce(um.basic_to_higher_upgrades, 0) + coalesce(um.pro_to_enterprise_upgrades, 0) as total_tier_upgrades,
		case 
			when (coalesce(um.basic_tier_accounts, 0) + coalesce(um.pro_tier_accounts, 0)) > 0 
			then round((coalesce(um.basic_to_higher_upgrades, 0) + coalesce(um.pro_to_enterprise_upgrades, 0)) * 100.0 / 
				(um.basic_tier_accounts + um.pro_tier_accounts), 2)
			else null
		end as lower_to_higher_tier_conversion_rate,
		
		-- best converting referral sources
		btc.best_trial_conversion_source,
		btc.best_trial_conversion_rate,
		btu.best_tier_upgrade_source,
		btu.best_tier_upgrade_rate

	from months m
	left join monthly_trial_metrics tm using (month_start)
	left join monthly_upgrade_metrics um using (month_start)
	left join best_trial_conversion_source btc using (month_start)
	left join best_tier_upgrade_source btu using (month_start)
	where m.month_start <= current_date()

)

select * from final
