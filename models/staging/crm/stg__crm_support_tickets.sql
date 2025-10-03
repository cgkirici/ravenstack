with source as (

    select * from {{ source('crm', 'ravenstack_support_tickets_with_text') }}

),

renamed as (

    select
        ticket_id,
        account_id,
        cast(submitted_at as timestamp) as submitted_at,
        cast(closed_at as timestamp) as closed_at,
        resolution_time_hours,
        priority,
        first_response_time_minutes,
        satisfaction_score,
        escalation_flag,
        subject,
        body

    from source

)

select * from renamed