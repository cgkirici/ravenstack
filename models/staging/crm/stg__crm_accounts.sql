with source as (
    
    select * from {{ source('crm', 'ravenstack_accounts') }}

),

renamed as (

    select
        account_id,
        account_name,
        industry,
        country,
        signup_date,
        referral_source,
        plan_tier,
        seats,
        is_trial,
        churn_flag as customer_churn_flag

    from source

)

select *
from renamed
