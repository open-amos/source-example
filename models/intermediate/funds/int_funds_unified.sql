with funds_resolved as (
    select
        fund_id,
        source_system,
        source_id,
        name,
        legal_name,
        type,
        vintage,
        management_fee,
        hurdle,
        carried_interest,
        target_commitment,
        base_currency_code,
        created_date,
        last_modified_date
    from {{ ref('int_funds_resolved') }}
),

-- Aggregate data by fund_id (currently only ADMIN source, but prepared for future sources)
funds_aggregated as (
    select
        fund_id,
        max(case when source_system = 'ADMIN' then name end) as admin_name,
        max(case when source_system = 'ADMIN' then legal_name end) as admin_legal_name,
        max(case when source_system = 'ADMIN' then type end) as admin_type,
        max(case when source_system = 'ADMIN' then vintage end) as admin_vintage,
        max(case when source_system = 'ADMIN' then management_fee end) as admin_management_fee,
        max(case when source_system = 'ADMIN' then hurdle end) as admin_hurdle,
        max(case when source_system = 'ADMIN' then carried_interest end) as admin_carried_interest,
        max(case when source_system = 'ADMIN' then target_commitment end) as admin_target_commitment,
        max(case when source_system = 'ADMIN' then base_currency_code end) as admin_base_currency_code,
        min(created_date) as earliest_created_date,
        max(last_modified_date) as latest_modified_date
    from funds_resolved
    group by fund_id
),

unified as (
    select
        fund_id,
        admin_name as name,
        admin_type as type,
        admin_vintage as vintage,
        admin_management_fee as management_fee,
        admin_hurdle as hurdle,
        admin_carried_interest as carried_interest,
        admin_target_commitment as target_commitment,
        null as incorporated_in,
        null as strategy,
        cast(null as date) as final_close_date,
        admin_base_currency_code as base_currency_code,
        earliest_created_date as created_at,
        latest_modified_date as updated_at
    from funds_aggregated
)

select * from unified
