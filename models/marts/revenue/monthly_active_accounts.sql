with subscriptions as (

    select * from {{ ref('stg__crm_subscriptions') }}

),

months as (

    {{ generate_month_spine('2023-01-01', '2024-12-31') }}

),

active_accounts_by_month as (

    select
        m.month_start as month_start,
        count(distinct s.account_id) as accounts_at_start,
        count(distinct s.subscription_id) as subscriptions_at_start,
        {%- set plan_tiers = ['Enterprise', 'Pro', 'Basic'] -%}
        {%- for tier in plan_tiers %}
            count(distinct case when s.plan_tier = '{{ tier }}' then s.account_id end) as {{ tier.lower() }}_at_start{% if not loop.last %},{% endif %}
        {%- endfor %}

    from months m
    left join subscriptions s
        on m.month_start >= date_trunc(s.start_date, month)
        and (s.end_date is null or m.month_start < date_trunc(s.end_date, month))
    where not s.is_trial
    group by 1
    having count(distinct s.account_id) > 0

),

final as (

    select
        active_accounts_by_month.*,
        
        accounts_at_start - lag(accounts_at_start) over (order by month_start) as accounts_change_from_previous_month,


    from active_accounts_by_month

)

select * from final
