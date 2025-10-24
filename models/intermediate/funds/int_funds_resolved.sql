with fund_admin_funds as (
    select
        fund_code as source_id,
        'ADMIN' as source_system,
        fund_name as name,
        fund_legal_name as legal_name,
        fund_type as type,
        vintage_year as vintage,
        management_fee_rate as management_fee,
        hurdle_rate as hurdle,
        carried_interest_rate as carried_interest,
        final_size as target_commitment,
        base_currency_code,
        created_date,
        last_modified_date
    from {{ ref('stg_fund_admin__funds') }}
),

all_sources as (
    select * from fund_admin_funds
),

xref as (
    select
        source_system,
        source_id,
        canonical_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FUND'
),

resolved as (
    select
        xref.canonical_id as fund_id,
        all_sources.source_system,
        all_sources.source_id,
        all_sources.name,
        all_sources.legal_name,
        all_sources.type,
        all_sources.vintage,
        all_sources.management_fee,
        all_sources.hurdle,
        all_sources.carried_interest,
        all_sources.target_commitment,
        all_sources.base_currency_code,
        all_sources.created_date,
        all_sources.last_modified_date
    from all_sources
    inner join xref
        on all_sources.source_system = xref.source_system
        and all_sources.source_id = xref.source_id
)

select * from resolved
