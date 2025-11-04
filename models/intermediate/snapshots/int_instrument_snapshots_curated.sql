with pm_instrument_snapshots as (
    select
        pm_instrument_snapshot_id,
        pm_instrument_id,
        period_start_date,
        period_end_date,
        frequency,
        reporting_basis,
        snapshot_source,
        currency_code,
        fx_rate,
        fx_rate_as_of,
        fx_rate_source,
        fair_value,
        accrued_income,
        equity_stake_pct,
        stake_basis,
        principal_outstanding,
        undrawn_commitment,
        accrued_interest,
        accrued_fees,
        amortized_cost,
        expected_loss,
        status,
        snapshot_source_file_ref,
        _source_system,
        _source_loaded_at
    from {{ ref('stg_pm__instrument_snapshots') }}
),

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
        {{ dbt_utils.generate_surrogate_key(['xref.instrument_id', 'pm.period_end_date', 'pm.reporting_basis', 'pm.snapshot_source']) }} as instrument_snapshot_id,
        xref.instrument_id,
        pm.period_start_date,
        pm.period_end_date,
        pm.frequency,
        pm.reporting_basis,
        pm.snapshot_source,
        pm.currency_code,
        pm.fx_rate,
        pm.fx_rate_as_of,
        pm.fx_rate_source,
        pm.fair_value,
        pm.amortized_cost,
        pm.principal_outstanding,
        pm.undrawn_commitment,
        pm.accrued_income,
        pm.accrued_interest,
        pm.accrued_fees,
        pm.equity_stake_pct,
        pm.stake_basis,
        pm.expected_loss,
        pm.status,
        pm.snapshot_source_file_ref,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from pm_instrument_snapshots pm
    inner join instrument_xref xref
        on pm.pm_instrument_id = xref.source_id
        and xref.source_system = 'PM'
)

select * from curated
