-- Build fund snapshots from admin_nav_fund
-- Maps fund admin NAV data to canonical fund snapshot structure
with admin_nav as (
    select
        nav_id,
        fund_code as source_fund_id,
        'ADMIN' as source_system,
        valuation_date as period_end_date,
        null as period_start_date,  -- Not provided in source
        'QUARTERLY' as frequency,  -- Assuming quarterly reporting
        'GAAP' as reporting_basis,  -- Assuming GAAP basis
        'ADMIN' as snapshot_source,
        committed_capital,
        called_capital,
        dpi_ratio as dpi,
        rvpi_ratio as rvpi,
        null as expected_coc,  -- Not provided in source
        remaining_value as cash_amount,
        distributed_capital as total_distributions,
        fund_expenses as total_expenses,
        management_fees_paid as total_management_fees,
        null as total_loans_received,  -- Not provided in source
        null as principal_outstanding,  -- Not provided in source
        (committed_capital - called_capital) as undrawn_commitment,
        null as total_interest_income,  -- Not provided in source
        created_date,
        last_modified_date
    from {{ ref('stg_fund_admin__nav_fund') }}
),

-- Resolve fund_id through xref
fund_xref as (
    select
        source_system,
        source_id,
        canonical_id as fund_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FUND'
),

curated as (
    select
        {{ dbt_utils.generate_surrogate_key(['fund_xref.fund_id', 'admin_nav.period_end_date', 'admin_nav.reporting_basis', 'admin_nav.snapshot_source']) }} as fund_snapshot_id,
        fund_xref.fund_id,
        admin_nav.period_start_date,
        admin_nav.period_end_date,
        admin_nav.frequency,
        admin_nav.reporting_basis,
        admin_nav.snapshot_source,
        admin_nav.committed_capital,
        admin_nav.called_capital,
        admin_nav.dpi,
        admin_nav.rvpi,
        admin_nav.expected_coc,
        admin_nav.cash_amount,
        admin_nav.total_distributions,
        admin_nav.total_expenses,
        admin_nav.total_management_fees,
        admin_nav.total_loans_received,
        admin_nav.principal_outstanding,
        admin_nav.undrawn_commitment,
        admin_nav.total_interest_income,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from admin_nav
    inner join fund_xref
        on admin_nav.source_system = fund_xref.source_system
        and admin_nav.source_fund_id = fund_xref.source_id
)

select * from curated
