-- -----------------------------------------------------------------------------
-- Purpose: Core fact table with monthly MRR snapshots at subscription level
-- Granularity: One row per subscription per month
-- -----------------------------------------------------------------------------

with subscriptions as (

    select * from {{ ref('stg__crm_subscriptions') }}

),

date_spine as (
  -- Monthly spine from earliest start to latest end (or today)
  select
    month_start
  from unnest(
    generate_date_array(
      (select date_trunc(min(start_date), month) from subscriptions),
      (select date_trunc(coalesce(max(end_date), current_date()), month) from subscriptions),
      interval 1 month
    )
  ) as month_start
),

subscription_months as (
  select
    s.subscription_id,
    s.account_id,
    s.plan_tier,
    s.seats,
    s.mrr_amount,
    s.start_date,
    s.end_date,
    s.is_trial,
    s.billing_frequency,
    d.month_start,

    -- Active in this month?
    case
      when d.month_start >= date_trunc(s.start_date, month)
           and (s.end_date is null or d.month_start <= date_trunc(s.end_date, month))
        then true
      else false
    end as is_active,

    -- First month?
    case
      when d.month_start = date_trunc(s.start_date, month)
        then true
      else false
    end as is_first_month,

    -- Last month (if churned)
    case
      when s.end_date is not null
           and d.month_start = date_trunc(s.end_date, month)
        then true
      else false
    end as is_last_month

  from subscriptions s
  cross join date_spine d
  where d.month_start >= date_trunc(s.start_date, month)
    and (s.end_date is null or d.month_start <= date_trunc(s.end_date, month))
),

mrr_calculated as (
  select
    sm.*,

    -- Calculated MRR (handle trials and fallback rates)
    case
      when sm.is_trial then 0
      when sm.mrr_amount > 0 then sm.mrr_amount
      when sm.plan_tier = 'Enterprise' then sm.seats * 100
      when sm.plan_tier = 'Pro'        then sm.seats * 50
      when sm.plan_tier = 'Basic'      then sm.seats * 25
      else 0
    end as calculated_mrr,

    case
      when sm.is_trial then 'Trial'
      when sm.mrr_amount = 0 then 'Non-Revenue'
      else 'Revenue'
    end as revenue_status

  from subscription_months sm
)

select
  {{ dbt_utils.generate_surrogate_key(['subscription_id', 'month_start']) }} as mrr_month_key,
  *
from mrr_calculated
where is_active = true
