with source as (

    select * from {{ source('crm', 'ravenstack_churn_events') }}

),

renamed as (

    select
        churn_event_id,
        account_id,
        churn_date,
        reason_code as churn_reason,
        refund_amount_usd,
        preceding_upgrade_flag,
        preceding_downgrade_flag,
        is_reactivation,
        feedback_text

    from source

)

select * from renamed
