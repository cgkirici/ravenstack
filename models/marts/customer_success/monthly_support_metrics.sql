with support_tickets as (

    select * from {{ ref('stg__crm_support_tickets') }}

),

support_metrics_by_month as (

    select
        cast(date_trunc(t.submitted_at, month) as date) as ticket_month,
        
        -- overall ticket metrics
        count(distinct t.ticket_id) as total_tickets,
        count(distinct t.account_id) as accounts_with_tickets,
        
        -- satisfaction metrics
        count(distinct case when t.satisfaction_score is not null then t.ticket_id end) as tickets_with_satisfaction,
        round(
            avg(case when t.satisfaction_score is not null then t.satisfaction_score end),
            2) as avg_satisfaction_score,
        
        -- low satisfaction (scores 1-3)
        count(distinct case when t.satisfaction_score <= 3 then t.ticket_id end) as low_satisfaction_tickets,
        count(distinct case when t.satisfaction_score <= 3 then t.account_id end) as accounts_with_low_satisfaction,
        
        -- high satisfaction (scores 4-5)
        count(distinct case when t.satisfaction_score >= 4 then t.ticket_id end) as high_satisfaction_tickets,
        
        -- response time metrics
        round(avg(t.first_response_time_minutes), 2) as avg_first_response_minutes,
        avg(t.resolution_time_hours) as avg_resolution_hours,
        
        -- escalations
        count(distinct case when t.escalation_flag then t.ticket_id end) as escalated_tickets,
        count(distinct case when t.escalation_flag then t.account_id end) as accounts_with_escalations,
        
        -- priority breakdown
        count(distinct case when t.priority = 'high' then t.ticket_id end) as high_priority_tickets
        
    from support_tickets t
    group by 1

)

select *
from support_metrics_by_month
order by ticket_month
