with counterparties_resolved as (
    select
        counterparty_id,
        source_system,
        source_id,
        name,
        type,
        country_code,
        created_date,
        last_modified_date
    from {{ ref('int_counterparties_resolved') }}
),

-- Aggregate data by counterparty_id with priority rules
-- Priority: PM > ADMIN (when multiple sources exist)
counterparties_aggregated as (
    select
        counterparty_id,
        max(case when source_system = 'PM' then name end) as pm_name,
        max(case when source_system = 'ADMIN' then name end) as admin_name,
        max(case when source_system = 'PM' then type end) as pm_type,
        max(case when source_system = 'ADMIN' then type end) as admin_type,
        max(case when source_system = 'PM' then country_code end) as pm_country_code,
        max(case when source_system = 'ADMIN' then country_code end) as admin_country_code,
        min(created_date) as earliest_created_date,
        max(last_modified_date) as latest_modified_date
    from counterparties_resolved
    group by counterparty_id
),

unified as (
    select
        counterparty_id,
        coalesce(pm_name, admin_name) as name,
        coalesce(pm_type, admin_type) as type,
        coalesce(pm_country_code, admin_country_code) as country_code,
        earliest_created_date as created_at,
        latest_modified_date as updated_at
    from counterparties_aggregated
)

select * from unified
