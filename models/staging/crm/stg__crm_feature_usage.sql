with source as (

    select * from {{ source('crm', 'ravenstack_feature_usage') }}

),

dupes as (
     
    select usage_id
    from source
    group by usage_id
    having count(*) = 1

),

renamed as (

    select
        usage_id,
        subscription_id,
        usage_date,
        feature_name,
        usage_count,
        usage_duration_secs,
        error_count,
        is_beta_feature

    from source
    inner join dupes using (usage_id)

)

select * from renamed
