-- -----------------------------------------------------------------------------
-- Purpose: Monthly MRR rollup with growth metrics
-- Granularity: One row per month
-- -----------------------------------------------------------------------------

with monthly_mrr as (
    select 
        month_start,
        
        -- Total MRR metrics
        sum(calculated_mrr) as total_mrr,
        sum(case when is_first_month and not is_trial then calculated_mrr else 0 end) as new_mrr,
        sum(case when is_last_month then calculated_mrr else 0 end) as churned_mrr,
        
        -- Count metrics
        count(distinct subscription_id) as total_subscriptions,
        count(distinct case when is_first_month then subscription_id end) as new_subscriptions,
        count(distinct case when is_last_month then subscription_id end) as churned_subscriptions,
        count(distinct account_id) as total_accounts,
        
        -- Trial vs Paid
        sum(case when is_trial then calculated_mrr else 0 end) as trial_mrr,
        sum(case when not is_trial then calculated_mrr else 0 end) as paid_mrr,
        count(distinct case when is_trial then subscription_id end) as trial_count,
        count(distinct case when not is_trial then subscription_id end) as paid_count

    from {{ ref('int_mrr_normalised') }}
    group by 1

),

with_growth_calcs as (

    select 
        *,
        
        -- Prior month comparisons
        lag(total_mrr, 1) over (order by month_start) as prior_month_mrr,
        lag(total_subscriptions, 1) over (order by month_start) as prior_month_subscriptions,
        
        -- MoM Growth calculations
        total_mrr - lag(total_mrr, 1) over (order by month_start) as mrr_change,
        round(
            ((total_mrr - lag(total_mrr, 1) over (order by month_start)) 
            / nullif(lag(total_mrr, 1) over (order by month_start), 0) * 100), 
            2
        ) as mrr_growth_rate_pct,
        
        -- Net MRR Movement
        new_mrr - churned_mrr as net_new_mrr,
        
        -- Average MRR per account
        round(total_mrr / nullif(total_accounts, 0), 2) as avg_mrr_per_account,
        round(paid_mrr / nullif(paid_count, 0), 2) as avg_mrr_per_paid_subscription
        
    from monthly_mrr
    
)

select * from with_growth_calcs
order by month_start
