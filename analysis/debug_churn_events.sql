select * from {{ ref('stg__crm_churn_events') }} where account_id = 'A-fd7ad3'
