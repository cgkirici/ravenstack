-- -----------------------------------------------------------------------------
-- Purpose: MRR segmented by subscription plan tier
-- Granularity: One row per month per plan tier
-- -----------------------------------------------------------------------------

{{ config(
    tags=['mrr', 'segmentation']
) }}

with mrr as (

    select * from {{ ref('int_mrr_normalised') }}

),

plan_mrr as (
    select 
        'Subscription Plan' as segment_type,

        plan_tier as segment_value,

        month_start,
        
        sum(calculated_mrr) as mrr,
        count(distinct subscription_id) as subscription_count,
        count(distinct account_id) as account_count,
        avg(calculated_mrr) as avg_mrr_per_subscription,
        sum(seats) as total_seats,
        round(avg(seats), 1) as avg_seats_per_subscription

    from mrr
    where not is_trial
    group by 1, 2, 3

),

with_totals as (
    select 
        month_start,
        sum(mrr) as total_month_mrr

    from plan_mrr
    group by 1

),

additional_metrics as (

    select 
        p.*,
        t.total_month_mrr,
        round((p.mrr / nullif(t.total_month_mrr, 0) * 100), 2) as pct_of_total_mrr,
        
        -- Growth vs prior month
        lag(p.mrr, 1) over (partition by p.segment_value order by p.month_start) as prior_month_mrr,
        p.mrr - lag(p.mrr, 1) over (partition by p.segment_value order by p.month_start) as mrr_change,
        round(
            ((p.mrr - lag(p.mrr, 1) over (partition by p.segment_value order by p.month_start)) 
            / nullif(lag(p.mrr, 1) over (partition by p.segment_value order by p.month_start), 0) * 100),
            2
        ) as mrr_growth_rate_pct
        
    from plan_mrr p
    left join with_totals t using (month_start)
    order by p.month_start, p.segment_type

)

select * from additional_metrics
