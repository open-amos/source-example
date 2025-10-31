-- Build loan snapshots from fund admin or PM sources
with pm_snapshots as (
    -- Extract loan snapshots from PM system
    select
        loan_snapshot_id,
        loan_id as source_loan_id,
        'PM' as source_system,
        cast(null as date) as period_start_date,
        period_end_date,
        'QUARTERLY' as frequency,
        'FAIR_VALUE' as reporting_basis,
        'PM_SYSTEM' as snapshot_source,
        principal_outstanding,
        undrawn_commitment,
        accrued_interest,
        cast(0 as numeric(18,2)) as accrued_fees,
        principal_outstanding as amortized_cost,
        fair_value,
        cast(0 as numeric(18,2)) as expected_loss,
        status,
        currency_code,
        cast(1.0 as numeric(18,6)) as fx_rate,
        principal_outstanding as principal_outstanding_converted,
        undrawn_commitment as undrawn_commitment_converted,
        accrued_interest as accrued_interest_converted,
        cast(0 as numeric(18,2)) as accrued_fees_converted,
        principal_outstanding as amortized_cost_converted,
        fair_value as fair_value_converted,
        cast(null as varchar(500)) as source_file_ref,
        created_date,
        last_modified_date
    from {{ ref('stg_pm__loan_snapshots') }}
),

-- Resolve loan_id through xref (when data exists)
loan_xref as (
    select
        source_system,
        source_id,
        canonical_id as loan_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'LOAN'
),

curated as (
    select
        {{ dbt_utils.generate_surrogate_key(['loan_xref.loan_id', 'pm_snapshots.period_end_date', 'pm_snapshots.reporting_basis', 'pm_snapshots.snapshot_source']) }} as loan_snapshot_id,
        loan_xref.loan_id,
        pm_snapshots.period_start_date,
        pm_snapshots.period_end_date,
        pm_snapshots.frequency,
        pm_snapshots.reporting_basis,
        pm_snapshots.snapshot_source,
        pm_snapshots.principal_outstanding,
        pm_snapshots.undrawn_commitment,
        pm_snapshots.accrued_interest,
        pm_snapshots.accrued_fees,
        pm_snapshots.amortized_cost,
        pm_snapshots.fair_value,
        pm_snapshots.expected_loss,
        pm_snapshots.status,
        pm_snapshots.currency_code,
        pm_snapshots.fx_rate,
        pm_snapshots.principal_outstanding_converted,
        pm_snapshots.undrawn_commitment_converted,
        pm_snapshots.accrued_interest_converted,
        pm_snapshots.accrued_fees_converted,
        pm_snapshots.amortized_cost_converted,
        pm_snapshots.fair_value_converted,
        pm_snapshots.source_file_ref,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from pm_snapshots
    inner join loan_xref
        on pm_snapshots.source_system = loan_xref.source_system
        and pm_snapshots.source_loan_id = loan_xref.source_id
)

select * from curated
