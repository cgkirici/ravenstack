-- -----------------------------------------------------------------------------
-- Purpose: MRR segmented by customer industry
-- Granularity: One row per month per industry
-- -----------------------------------------------------------------------------

{{ config(
    tags=['mrr', 'segmentation']
) }}

with mrr as (

    select * from {{ ref('int_mrr_normalised') }}

),

accounts as (
    
    select * from {{ ref('stg__crm_accounts') }}

),

industry_mrr as (
    
    select 
        'Customer Industry' as segment_type,
        a.industry as segment_value,

        mrr.month_start,

        sum(mrr.calculated_mrr) as mrr,
        count(distinct mrr.subscription_id) as subscription_count,
        count(distinct mrr.account_id) as account_count,
        avg(mrr.calculated_mrr) as avg_mrr_per_subscription,
        
        -- Plan tier mix within industry
        count(distinct case when mrr.plan_tier = 'Enterprise' then mrr.subscription_id end) as enterprise_count,
        count(distinct case when mrr.plan_tier = 'Pro' then mrr.subscription_id end) as pro_count,
        count(distinct case when mrr.plan_tier = 'Basic' then mrr.subscription_id end) as basic_count
        
    from mrr
    left join accounts a using (account_id)
    where not mrr.is_trial
    group by 1, 2, 3

),

additional_metrics as (

select 
    *,
    lag(mrr, 1) over (partition by segment_type order by month_start) as prior_month_mrr,
    mrr - lag(mrr, 1) over (partition by segment_type order by month_start) as mrr_change,
    round(
        ((mrr - lag(mrr, 1) over (partition by segment_type order by month_start)) 
        / nullif(lag(mrr, 1) over (partition by segment_type order by month_start), 0) * 100),
        2
    ) as mrr_growth_rate_pct

from industry_mrr
order by month_start, segment_value

)

select * from additional_metrics
