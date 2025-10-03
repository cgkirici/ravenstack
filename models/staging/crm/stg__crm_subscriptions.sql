with source as (

    select * from {{ source('crm', 'ravenstack_subscriptions') }}

),

renamed as (

    select
        subscription_id,
        account_id,
        start_date,
        end_date,
        plan_tier,
        seats,
        mrr_amount,
        arr_amount,
        is_trial,
        upgrade_flag,
        downgrade_flag,
        churn_flag,
        billing_frequency,
        auto_renew_flag

    from source

)

select * from renamed
