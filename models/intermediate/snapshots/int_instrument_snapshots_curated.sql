-- Build instrument snapshots from admin_nav_investment and pm_valuations
-- Combines NAV data from fund admin and valuation data from PM system
with admin_nav as (
    select
        nav_investment_id,
        investment_id as source_investment_id,
        fund_code as source_fund_id,
        'ADMIN' as source_system,
        valuation_date as period_end_date,
        null as period_start_date,  -- Not provided in source
        'QUARTERLY' as frequency,  -- Assuming quarterly reporting
        'GAAP' as reporting_basis,  -- Assuming GAAP basis
        'ADMIN' as snapshot_source,
        fair_value_currency as currency_code,
        cast(null as numeric) as fx_rate,  -- Not provided in source
        cast(fair_value as numeric) as fair_value,
        cast(cost_basis as numeric) as amortized_cost,
        cast(null as numeric) as principal_outstanding,  -- Not applicable for equity
        cast(null as numeric) as undrawn_commitment,  -- Not provided in source
        cast(null as numeric) as accrued_income,  -- Not provided in source
        cast(null as numeric) as accrued_fees,  -- Not provided in source
        cast(null as numeric) as fair_value_converted,  -- Will be calculated in marts
        cast(null as numeric) as amortized_cost_converted,  -- Will be calculated in marts
        cast(null as numeric) as principal_outstanding_converted,
        cast(null as numeric) as undrawn_commitment_converted,
        cast(null as numeric) as accrued_income_converted,
        cast(null as numeric) as accrued_fees_converted,
        cast(ownership_percentage as numeric) as equity_stake_pct,
        cast(null as numeric) as equity_dividends_cum,  -- Not provided in source
        cast(null as numeric) as equity_exit_proceeds_actual,  -- Not provided in source
        cast(null as numeric) as equity_exit_proceeds_forecast,  -- Not provided in source
        cast(null as text) as snapshot_source_file_ref,  -- Not provided in source
        created_date,
        last_modified_date
    from {{ ref('stg_fund_admin__nav_investment') }}
),

pm_valuations as (
    select
        valuation_id,
        investment_id as source_investment_id,
        cast(null as text) as source_fund_id,  -- Not provided in PM valuations
        'PM' as source_system,
        valuation_date as period_end_date,
        null as period_start_date,  -- Not provided in source
        'QUARTERLY' as frequency,  -- Assuming quarterly reporting
        'GAAP' as reporting_basis,  -- Assuming GAAP basis
        'INTERNAL' as snapshot_source,
        equity_value_currency as currency_code,
        cast(null as numeric) as fx_rate,  -- Not provided in source
        cast(equity_value as numeric) as fair_value,
        cast(null as numeric) as amortized_cost,  -- Not provided in PM valuations
        cast(null as numeric) as principal_outstanding,
        cast(null as numeric) as undrawn_commitment,
        cast(null as numeric) as accrued_income,
        cast(null as numeric) as accrued_fees,
        cast(null as numeric) as fair_value_converted,
        cast(null as numeric) as amortized_cost_converted,
        cast(null as numeric) as principal_outstanding_converted,
        cast(null as numeric) as undrawn_commitment_converted,
        cast(null as numeric) as accrued_income_converted,
        cast(null as numeric) as accrued_fees_converted,
        cast(null as numeric) as equity_stake_pct,  -- Not provided in PM valuations
        cast(null as numeric) as equity_dividends_cum,
        cast(null as numeric) as equity_exit_proceeds_actual,
        cast(null as numeric) as equity_exit_proceeds_forecast,
        cast(valuation_source as text) as snapshot_source_file_ref,
        created_date,
        last_modified_date
    from {{ ref('stg_pm__valuations') }}
),

all_sources as (
    select * from admin_nav
    union all
    select * from pm_valuations
),

-- Resolve instrument_id through xref
instrument_xref as (
    select
        source_system,
        source_id,
        canonical_id as instrument_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INSTRUMENT'
),

curated as (
    select
        {{ dbt_utils.generate_surrogate_key(['instrument_xref.instrument_id', 'all_sources.period_end_date', 'all_sources.reporting_basis', 'all_sources.snapshot_source']) }} as instrument_snapshot_id,
        instrument_xref.instrument_id,
        all_sources.period_start_date,
        all_sources.period_end_date,
        all_sources.frequency,
        all_sources.reporting_basis,
        all_sources.snapshot_source,
        all_sources.currency_code,
        all_sources.fx_rate,
        all_sources.fair_value,
        all_sources.amortized_cost,
        all_sources.principal_outstanding,
        all_sources.undrawn_commitment,
        all_sources.accrued_income,
        all_sources.accrued_fees,
        all_sources.fair_value_converted,
        all_sources.amortized_cost_converted,
        all_sources.principal_outstanding_converted,
        all_sources.undrawn_commitment_converted,
        all_sources.accrued_income_converted,
        all_sources.accrued_fees_converted,
        all_sources.equity_stake_pct,
        all_sources.equity_dividends_cum,
        all_sources.equity_exit_proceeds_actual,
        all_sources.equity_exit_proceeds_forecast,
        all_sources.snapshot_source_file_ref,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from all_sources
    inner join instrument_xref
        on all_sources.source_system = instrument_xref.source_system
        and all_sources.source_investment_id = instrument_xref.source_id
)

select * from curated
