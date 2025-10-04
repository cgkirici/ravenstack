-- -----------------------------------------------------------------------------
-- purpose: monthly churn rate calculation with support metrics correlation
-- granularity: one row per month
-- -----------------------------------------------------------------------------

with churn as (

    select * from {{ ref('monthly_churned_accounts') }}

),

support_metrics as (

    select * from {{ ref('monthly_support_metrics') }}

),

active_accounts as (
    
    select * from {{ ref('monthly_active_accounts') }}

),

months as (

    {{ generate_month_spine('2023-01-01') }}

),

combined_metrics as (

    select
        aa.month_start,
        
        -- account counts
        aa.accounts_at_start,
        aa.subscriptions_at_start,
        aa.enterprise_at_start,
        aa.pro_at_start,
        aa.basic_at_start,
        
        -- churn counts
        {{ dbt_utils.star(
            from=ref('monthly_churned_accounts'), 
            except=['month_start'],
            relation_alias='ca'
            ) }},
        
        -- support metrics
        coalesce(sm.total_tickets, 0) as total_tickets,
        coalesce(sm.accounts_with_tickets, 0) as accounts_with_tickets,
        sm.avg_satisfaction_score,
        coalesce(sm.low_satisfaction_tickets, 0) as low_satisfaction_tickets,
        coalesce(sm.accounts_with_low_satisfaction, 0) as accounts_with_low_satisfaction,
        coalesce(sm.high_satisfaction_tickets, 0) as high_satisfaction_tickets,
        sm.avg_first_response_minutes,
        sm.avg_resolution_hours,
        coalesce(sm.escalated_tickets, 0) as escalated_tickets,
        coalesce(sm.accounts_with_escalations, 0) as accounts_with_escalations,
        coalesce(sm.high_priority_tickets, 0) as high_priority_tickets
        
    from active_accounts aa
    left join churn ca using (month_start)
    left join support_metrics sm on aa.month_start = sm.ticket_month

),

final as (

    select
        month_start,
        
        -- account metrics
        accounts_at_start, 
        subscriptions_at_start,
        churned_accounts,
        cancelled_subscriptions,
        enterprise_at_start,
        pro_at_start,
        basic_at_start,
        
        -- churn rates (%)
        round(safe_divide(churned_accounts, accounts_at_start), 2) as churn_rate,
        round(safe_divide(cancelled_subscriptions, nullif(subscriptions_at_start, 0)), 2) as cancel_rate_subscriptions,
        round(safe_divide(cancelled_enterprise, nullif(enterprise_at_start, 0)), 2) as cancel_rate_enterprise,
        round(safe_divide(cancelled_pro, nullif(pro_at_start, 0)), 2) as cancel_rate_pro,
        round(safe_divide(cancelled_basic, nullif(basic_at_start, 0)), 2) as cancel_rate_basic,
        
        -- churn by reason (counts and percentages)
        {# churned_budget,
        churned_competitor,
        churned_features, #}
        {%- set churn_reason_columns = adapter.get_columns_in_relation(ref('monthly_churned_accounts')) -%}
        {%- for column in churn_reason_columns -%}
            {%- if column.name.startswith('cancelled_') and column.name != 'cancelled_subscriptions' -%}
        round(safe_divide({{ column.name }} * 100.0, nullif(cancelled_subscriptions, 0)), 2) as pct_{{ column.name }},
            {%- endif -%}
        {%- endfor -%}
        
        -- financial impact
        total_refunds,
        round(safe_divide(total_refunds, nullif(churned_accounts, 0)), 2) as avg_refund_per_churned_account,
        
        -- churn behavioral flags
        churned_after_upgrade,
        churned_after_downgrade,
        round(safe_divide(churned_after_upgrade * 100.0, nullif(churned_accounts, 0)), 2) as pct_churned_after_upgrade,
        
        -- support metrics
        total_tickets,
        accounts_with_tickets,
        round(safe_divide(accounts_with_tickets * 100.0, accounts_at_start), 2) as pct_accounts_with_tickets,
        round(avg_satisfaction_score, 2) as avg_satisfaction_score,
        low_satisfaction_tickets,
        accounts_with_low_satisfaction,
        high_satisfaction_tickets,
        round(avg_first_response_minutes, 1) as avg_first_response_minutes,
        round(avg_resolution_hours, 1) as avg_resolution_hours,
        escalated_tickets,
        accounts_with_escalations,
        
        -- correlation indicators
        round(safe_divide(accounts_with_low_satisfaction * 100.0, nullif(accounts_with_tickets, 0)), 2) as pct_tickets_low_satisfaction,
        round(safe_divide(escalated_tickets * 100.0, nullif(total_tickets, 0)), 2) as pct_tickets_escalated,
        
        -- risk indicators (accounts with low satisfaction as % of churned accounts)
        case 
            when churned_accounts > 0 and accounts_with_low_satisfaction > 0 then 
            round(safe_divide(accounts_with_low_satisfaction * 100.0, nullif(churned_accounts, 0)), 2)
            else null 
        end as low_satisfaction_to_churn_ratio,
        
        -- 3-month rolling averages for trend smoothing
        round(avg(safe_divide(churned_accounts * 100.0, accounts_at_start)) over (
            order by month_start 
            rows between 2 preceding and current row
        ), 2) as churn_rate_3m_avg,
        
        round(avg(avg_satisfaction_score) over (
            order by month_start 
            rows between 2 preceding and current row
        ), 2) as satisfaction_score_3m_avg

    from combined_metrics
    where accounts_at_start > 0
    order by month_start

)

select * from final
