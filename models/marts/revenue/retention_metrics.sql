with accounts as (

    select *
    from {{ ref('stg__crm_accounts') }}

),

feature_usage as (

    select *
    from {{ ref('stg__crm_feature_usage') }}

),

subscriptions as (

    select *
    from {{ ref('stg__crm_subscriptions') }}

),

account_status as (

	select
		a.*,

		case
			when a.customer_churn_flag = true then 'churned'
			else 'retained'
		end as customer_status,

		date_trunc(a.signup_date, month) as signup_cohort_month
		
	from accounts a

),

feature_usage_aggreations as (

	select
		fu.feature_name,
		s.account_id,
    
		count(distinct fu.usage_id) as total_usage_events,
		sum(fu.usage_count) as total_usage_count,
		sum(fu.usage_duration_secs) as total_usage_duration_secs,
		sum(fu.error_count) as total_error_count,
		min(fu.usage_date) as first_usage_date,
		max(fu.usage_date) as last_usage_date,
		count(distinct date_trunc(fu.usage_date, month)) as months_with_usage,
		max(fu.is_beta_feature) as is_beta_feature
		
	from feature_usage fu
    left join subscriptions s using (subscription_id)
	group by 1, 2

),

account_feature_metrics as (

	select
		fub.account_id,
		fub.feature_name,
		fub.total_usage_events,
		fub.total_usage_count,
		fub.total_usage_duration_secs,
		fub.total_error_count,
		fub.first_usage_date,
		fub.last_usage_date,
		fub.months_with_usage,
		fub.is_beta_feature,
		
		-- Usage intensity
		round(fub.total_usage_duration_secs / 60.0, 2) as total_usage_minutes,
		round(
			fub.total_usage_duration_secs / nullif(fub.total_usage_events, 0),
			2
		) as avg_duration_per_event_secs,
		
		-- Error rate
		round(
			fub.total_error_count * 100.0 / nullif(fub.total_usage_count, 0),
			2
		) as error_rate_pct,
		
		-- Usage recency (days since last use)
		date_diff(current_date(), fub.last_usage_date, day) as days_since_last_use,
		
		-- Usage frequency
		case
			when fub.months_with_usage >= 6 then 'high_frequency'
			when fub.months_with_usage >= 3 then 'medium_frequency'
			when fub.months_with_usage >= 1 then 'low_frequency'
			else 'single_use'
		end as usage_frequency_bucket
		
	from feature_usage_aggreations fub

),

account_total_usage as (

	select
		account_id,
		count(distinct feature_name) as total_features_used,
		sum(total_usage_events) as total_usage_events_all_features,
		sum(total_usage_duration_secs) as total_usage_duration_all_features
		
	from account_feature_metrics
	group by 1

),

feature_penetration as (

	select
		afm.feature_name,
		ast.customer_status,

		count(distinct afm.account_id) as accounts_using_feature,
		sum(afm.total_usage_events) as total_feature_usage_events,
		sum(afm.total_usage_duration_secs) as total_feature_usage_duration_secs,
		avg(afm.total_usage_count) as avg_usage_count_per_account,
		avg(afm.total_usage_duration_secs) as avg_usage_duration_per_account,
		avg(afm.months_with_usage) as avg_months_with_usage,
		sum(afm.total_error_count) as total_feature_errors,
		max(afm.is_beta_feature) as is_beta_feature
		
	from account_feature_metrics afm
	left join account_status ast on afm.account_id = ast.account_id
	group by 1, 2

),

retained_vs_churned_comparison as (

	select
		feature_name,
        
		max(is_beta_feature) as is_beta_feature,
		
		-- Retained customer metrics
		max(case when customer_status = 'retained' then accounts_using_feature else 0 end) as retained_users,
		max(case when customer_status = 'retained' then total_feature_usage_events else 0 end) as retained_usage_events,
		max(case when customer_status = 'retained' then avg_usage_count_per_account else 0 end) as retained_avg_usage_count,
		max(case when customer_status = 'retained' then avg_usage_duration_per_account else 0 end) as retained_avg_duration,
		max(case when customer_status = 'retained' then avg_months_with_usage else 0 end) as retained_avg_months_active,
		
		-- Churned customer metrics
		max(case when customer_status = 'churned' then accounts_using_feature else 0 end) as churned_users,
		max(case when customer_status = 'churned' then total_feature_usage_events else 0 end) as churned_usage_events,
		max(case when customer_status = 'churned' then avg_usage_count_per_account else 0 end) as churned_avg_usage_count,
		max(case when customer_status = 'churned' then avg_usage_duration_per_account else 0 end) as churned_avg_duration,
		max(case when customer_status = 'churned' then avg_months_with_usage else 0 end) as churned_avg_months_active,
		
		-- Total across both groups
		sum(accounts_using_feature) as total_users,
		sum(total_feature_usage_events) as total_usage_events
		
	from feature_penetration
	group by 1

),

total_customer_counts as (

	select
		customer_status,
		count(distinct account_id) as total_customers
		
	from account_status
	group by 1

),

final as (

    select
        rvc.feature_name,
        rvc.is_beta_feature,
        
        -- Retained customer metrics
        rvc.retained_users,
        tcc_retained.total_customers as total_retained_customers,
        round(
            rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0),
            2
        ) as retained_penetration_pct,
        round(rvc.retained_avg_usage_count, 2) as retained_avg_usage_count,
        round(rvc.retained_avg_duration / 60.0, 2) as retained_avg_usage_minutes,
        round(rvc.retained_avg_months_active, 2) as retained_avg_months_active,
        
        -- Churned customer metrics
        rvc.churned_users,
        tcc_churned.total_customers as total_churned_customers,
        round(
            rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0),
            2
        ) as churned_penetration_pct,
        round(rvc.churned_avg_usage_count, 2) as churned_avg_usage_count,
        round(rvc.churned_avg_duration / 60.0, 2) as churned_avg_usage_minutes,
        round(rvc.churned_avg_months_active, 2) as churned_avg_months_active,
        
        -- Comparison metrics
        rvc.total_users,
        round(
            rvc.retained_users * 100.0 / nullif(rvc.total_users, 0),
            2
        ) as pct_users_retained,
        
        -- Penetration difference (retained vs churned)
        round(
            (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
            (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)),
            2
        ) as penetration_diff_pct,
        
        -- Usage intensity difference (retained vs churned)
        round(
            rvc.retained_avg_usage_count - rvc.churned_avg_usage_count,
            2
        ) as usage_count_diff,
        
        round(
            (rvc.retained_avg_duration - rvc.churned_avg_duration) / 60.0,
            2
        ) as usage_duration_diff_minutes,
        
        -- Stickiness score (0-100)
        -- Higher score = more sticky (higher retention correlation)
        round(
            (
                -- Component 1: Penetration difference (0-40 points)
                case
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) >= 30 then 40
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) >= 20 then 30
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) >= 10 then 20
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) > 0 then 10
                    else 0
                end +
                
                -- Component 2: Usage frequency difference (0-30 points)
                case
                    when rvc.retained_avg_months_active - rvc.churned_avg_months_active >= 4 then 30
                    when rvc.retained_avg_months_active - rvc.churned_avg_months_active >= 2 then 20
                    when rvc.retained_avg_months_active - rvc.churned_avg_months_active >= 1 then 10
                    else 0
                end +
                
                -- Component 3: Retention rate among users (0-30 points)
                case
                    when rvc.retained_users * 100.0 / nullif(rvc.total_users, 0) >= 90 then 30
                    when rvc.retained_users * 100.0 / nullif(rvc.total_users, 0) >= 80 then 20
                    when rvc.retained_users * 100.0 / nullif(rvc.total_users, 0) >= 70 then 10
                    else 0
                end
            ),
            2
        ) as stickiness_score,
        
        -- Stickiness category
        case
            when (
                case
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) >= 30 then 40
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) >= 20 then 30
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) >= 10 then 20
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) > 0 then 10
                    else 0
                end +
                case
                    when rvc.retained_avg_months_active - rvc.churned_avg_months_active >= 4 then 30
                    when rvc.retained_avg_months_active - rvc.churned_avg_months_active >= 2 then 20
                    when rvc.retained_avg_months_active - rvc.churned_avg_months_active >= 1 then 10
                    else 0
                end +
                case
                    when rvc.retained_users * 100.0 / nullif(rvc.total_users, 0) >= 90 then 30
                    when rvc.retained_users * 100.0 / nullif(rvc.total_users, 0) >= 80 then 20
                    when rvc.retained_users * 100.0 / nullif(rvc.total_users, 0) >= 70 then 10
                    else 0
                end
            ) >= 70 then 'highly_sticky'
            when (
                case
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) >= 30 then 40
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) >= 20 then 30
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) >= 10 then 20
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) > 0 then 10
                    else 0
                end +
                case
                    when rvc.retained_avg_months_active - rvc.churned_avg_months_active >= 4 then 30
                    when rvc.retained_avg_months_active - rvc.churned_avg_months_active >= 2 then 20
                    when rvc.retained_avg_months_active - rvc.churned_avg_months_active >= 1 then 10
                    else 0
                end +
                case
                    when rvc.retained_users * 100.0 / nullif(rvc.total_users, 0) >= 90 then 30
                    when rvc.retained_users * 100.0 / nullif(rvc.total_users, 0) >= 80 then 20
                    when rvc.retained_users * 100.0 / nullif(rvc.total_users, 0) >= 70 then 10
                    else 0
                end
            ) >= 40 then 'moderately_sticky'
            when (
                case
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) >= 30 then 40
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) >= 20 then 30
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) >= 10 then 20
                    when (rvc.retained_users * 100.0 / nullif(tcc_retained.total_customers, 0)) -
                        (rvc.churned_users * 100.0 / nullif(tcc_churned.total_customers, 0)) > 0 then 10
                    else 0
                end +
                case
                    when rvc.retained_avg_months_active - rvc.churned_avg_months_active >= 4 then 30
                    when rvc.retained_avg_months_active - rvc.churned_avg_months_active >= 2 then 20
                    when rvc.retained_avg_months_active - rvc.churned_avg_months_active >= 1 then 10
                    else 0
                end +
                case
                    when rvc.retained_users * 100.0 / nullif(rvc.total_users, 0) >= 90 then 30
                    when rvc.retained_users * 100.0 / nullif(rvc.total_users, 0) >= 80 then 20
                    when rvc.retained_users * 100.0 / nullif(rvc.total_users, 0) >= 70 then 10
                    else 0
                end
            ) > 0 then 'low_stickiness'
            else 'not_sticky'
        end as stickiness_category

    from retained_vs_churned_comparison rvc
    cross join (select total_customers from total_customer_counts where customer_status = 'retained') tcc_retained
    cross join (select total_customers from total_customer_counts where customer_status = 'churned') tcc_churned
    order by stickiness_score desc

)

select *
from final
