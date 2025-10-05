-- Comprehensive analysis of account A-e7a1e2 data quality issues
-- Comparing subscription end dates vs churn events

with subscription_analysis as (
  select 
    account_id,
    subscription_id,
    plan_tier,
    seats,
    start_date,
    end_date,
    case when end_date is not null then 'ENDED' else 'ACTIVE' end as subscription_status,
    upgrade_flag,
    downgrade_flag
  from {{ ref('stg__crm_subscriptions') }}
  where account_id = 'A-e7a1e2'
),

churn_analysis as (
  select 
    account_id,
    churn_date,
    reason_code,
    refund_amount_usd,
    preceding_upgrade_flag,
    preceding_downgrade_flag,
    is_reactivation
  from {{ ref('stg__crm_churn_events') }}
  where account_id = 'A-e7a1e2'
),

-- Find ended subscriptions that don't have corresponding churn events
ended_subscriptions_without_churn as (
  select 
    s.account_id,
    s.subscription_id,
    s.end_date as subscription_end_date,
    s.plan_tier,
    s.seats,
    case 
      when c.churn_date is null then 'NO_CHURN_RECORD'
      when s.end_date != c.churn_date then 'DATE_MISMATCH'
      else 'MATCH'
    end as data_quality_issue
  from subscription_analysis s
  left join churn_analysis c on s.account_id = c.account_id 
    and date(s.end_date) = date(c.churn_date)
  where s.subscription_status = 'ENDED'
),

-- Summary of data quality issues
summary as (
  select
    'SUBSCRIPTION_DATA' as analysis_type,
    count(*) as total_subscriptions,
    sum(case when subscription_status = 'ENDED' then 1 else 0 end) as ended_subscriptions,
    sum(case when subscription_status = 'ACTIVE' then 1 else 0 end) as active_subscriptions
  from subscription_analysis
  
  union all
  
  select
    'CHURN_DATA' as analysis_type,
    count(*) as total_records,
    0 as ended_subscriptions,
    0 as active_subscriptions
  from churn_analysis
  
  union all
  
  select
    'DATA_QUALITY_ISSUES' as analysis_type,
    count(*) as total_ended_subscriptions,
    sum(case when data_quality_issue = 'NO_CHURN_RECORD' then 1 else 0 end) as missing_churn_records,
    sum(case when data_quality_issue = 'DATE_MISMATCH' then 1 else 0 end) as date_mismatches
  from ended_subscriptions_without_churn
)

-- Return all analysis results
select 'SUBSCRIPTIONS' as section, * from subscription_analysis
union all
select 'CHURN_EVENTS' as section, cast(account_id as string), cast(churn_date as string), reason_code, cast(refund_amount_usd as string), cast(preceding_upgrade_flag as string), cast(preceding_downgrade_flag as string), cast(is_reactivation as string), null, null from churn_analysis
union all  
select 'QUALITY_ISSUES' as section, cast(account_id as string), subscription_id, cast(subscription_end_date as string), plan_tier, cast(seats as string), data_quality_issue, null, null, null from ended_subscriptions_without_churn
union all
select 'SUMMARY' as section, analysis_type, cast(total_subscriptions as string), cast(ended_subscriptions as string), cast(active_subscriptions as string), null, null, null, null, null from summary

order by section, subscription_id
