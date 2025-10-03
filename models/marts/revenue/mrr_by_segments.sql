{{ dbt_utils.union_relations(
    relations=[ref('int_mrr_by_industry'), ref('int_mrr_by_subscription_plan')]
) }}