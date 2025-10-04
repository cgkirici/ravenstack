with churn_events as (

    select * from {{ ref('stg__crm_churn_events') }}

),

accounts as (
    
    select * from {{ ref('stg__crm_accounts') }}

),

months as (

    {{ generate_month_spine('2023-01-01') }}

),

churned_accounts_by_month as (

    select
        date_trunc(c.churn_date, month) as month_start,

        count(distinct case when a.customer_churn_flag is true then c.account_id else null end) as churned_accounts,
        count(distinct c.account_id) as cancelled_subscriptions,
        
        -- churn by reason
        {%- set churn_reasons_query %}
            select distinct churn_reason 
            from {{ ref('stg__crm_churn_events') }} 
            where churn_reason is not null
        {%- endset %}
        {%- set results = run_query(churn_reasons_query) %}
        {%- if execute %}
            {%- set churn_reasons = results.columns[0].values() %}
        {%- else %}
            {%- set churn_reasons = [] %}
        {%- endif %}
        {%- for reason in churn_reasons %}
        count(distinct case when c.churn_reason = '{{ reason }}' then c.account_id end) as cancelled_for_{{ reason }},
        {%- endfor %}
        
        -- churn by plan tier
        {%- set plan_tiers_query %}
            select distinct plan_tier 
            from {{ ref('stg__crm_accounts') }} 
            where plan_tier is not null
        {%- endset %}
        {%- set results = run_query(plan_tiers_query) %}
        {%- if execute %}
            {%- set plan_tiers = results.columns[0].values() %}
        {%- else %}
            {%- set plan_tiers = [] %}
        {%- endif %}
        {%- for tier in plan_tiers %}
        count(distinct case when a.plan_tier = '{{ tier }}' then c.account_id end) as cancelled_{{ tier | lower }},
        {%- endfor %}
        
        -- revenue impact
        sum(c.refund_amount_usd) as total_refunds,
        
        -- behavioral flags
        count(distinct case when c.preceding_upgrade_flag then c.account_id end) as churned_after_upgrade,
        count(distinct case when c.preceding_downgrade_flag then c.account_id end) as churned_after_downgrade
        
    from churn_events c
    left join accounts a on c.account_id = a.account_id
    group by 1

)

select *
from churned_accounts_by_month
order by month_start
