-- Build loan snapshots from fund admin or PM sources
-- Note: Current seed data doesn't have explicit loan snapshots, so this is a placeholder
-- In a real implementation, this would pull from loan-specific NAV tables
with loan_snapshots_placeholder as (
    -- This CTE would typically pull from a loan-specific NAV table
    -- For now, we create an empty structure matching the canonical schema
    select
        cast(null as varchar(64)) as loan_snapshot_id,
        cast(null as varchar(64)) as source_loan_id,
        cast(null as varchar(50)) as source_system,
        cast(null as date) as period_start_date,
        cast(null as date) as period_end_date,
        cast(null as varchar(50)) as frequency,
        cast(null as varchar(50)) as reporting_basis,
        cast(null as varchar(50)) as snapshot_source,
        cast(null as numeric(18,2)) as principal_outstanding,
        cast(null as numeric(18,2)) as undrawn_commitment,
        cast(null as numeric(18,2)) as accrued_interest,
        cast(null as numeric(18,2)) as accrued_fees,
        cast(null as numeric(18,2)) as amortized_cost,
        cast(null as numeric(18,2)) as fair_value,
        cast(null as numeric(18,2)) as expected_loss,
        cast(null as varchar(50)) as status,
        cast(null as char(3)) as currency_code,
        cast(null as numeric(18,6)) as fx_rate,
        cast(null as numeric(18,2)) as principal_outstanding_converted,
        cast(null as numeric(18,2)) as undrawn_commitment_converted,
        cast(null as numeric(18,2)) as accrued_interest_converted,
        cast(null as numeric(18,2)) as accrued_fees_converted,
        cast(null as numeric(18,2)) as amortized_cost_converted,
        cast(null as numeric(18,2)) as fair_value_converted,
        cast(null as varchar(500)) as source_file_ref,
        cast(null as date) as created_date,
        cast(null as date) as last_modified_date
    where 1 = 0  -- Return empty result set
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
        {{ dbt_utils.generate_surrogate_key(['loan_xref.loan_id', 'loan_snapshots_placeholder.period_end_date', 'loan_snapshots_placeholder.reporting_basis', 'loan_snapshots_placeholder.snapshot_source']) }} as loan_snapshot_id,
        loan_xref.loan_id,
        loan_snapshots_placeholder.period_start_date,
        loan_snapshots_placeholder.period_end_date,
        loan_snapshots_placeholder.frequency,
        loan_snapshots_placeholder.reporting_basis,
        loan_snapshots_placeholder.snapshot_source,
        loan_snapshots_placeholder.principal_outstanding,
        loan_snapshots_placeholder.undrawn_commitment,
        loan_snapshots_placeholder.accrued_interest,
        loan_snapshots_placeholder.accrued_fees,
        loan_snapshots_placeholder.amortized_cost,
        loan_snapshots_placeholder.fair_value,
        loan_snapshots_placeholder.expected_loss,
        loan_snapshots_placeholder.status,
        loan_snapshots_placeholder.currency_code,
        loan_snapshots_placeholder.fx_rate,
        loan_snapshots_placeholder.principal_outstanding_converted,
        loan_snapshots_placeholder.undrawn_commitment_converted,
        loan_snapshots_placeholder.accrued_interest_converted,
        loan_snapshots_placeholder.accrued_fees_converted,
        loan_snapshots_placeholder.amortized_cost_converted,
        loan_snapshots_placeholder.fair_value_converted,
        loan_snapshots_placeholder.source_file_ref,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from loan_snapshots_placeholder
    left join loan_xref
        on loan_snapshots_placeholder.source_system = loan_xref.source_system
        and loan_snapshots_placeholder.source_loan_id = loan_xref.source_id
)

select * from curated
