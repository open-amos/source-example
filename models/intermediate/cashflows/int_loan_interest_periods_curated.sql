-- Build loan interest periods from fund admin sources
-- Calculates interest accrual periods for loans
-- Note: Currently no loan source data exists in seeds
-- This model will return empty results until loan data is added

with fund_admin_interest_periods as (
    -- Placeholder: Extract interest periods from fund admin system when available
    -- Expected columns: loan_id, period_start, period_end, index_rate_pct, margin_pct,
    -- accrual_days, expected_interest_amount, payment_due_date, actual_interest_amount
    select
        cast(null as varchar(64)) as source_id,
        cast(null as varchar(10)) as source_system,
        cast(null as varchar(64)) as loan_id,
        cast(null as date) as period_start,
        cast(null as date) as period_end,
        cast(null as decimal(9,6)) as index_rate_pct,
        cast(null as decimal(9,6)) as margin_pct,
        cast(null as integer) as accrual_days,
        cast(null as numeric(24,2)) as expected_interest_amount,
        cast(null as date) as payment_due_date,
        cast(null as numeric(24,2)) as actual_interest_amount,
        cast(null as timestamp) as created_date,
        cast(null as timestamp) as last_modified_date
    where 1 = 0  -- Return no rows until source data exists
),

pm_interest_periods as (
    -- Placeholder: Extract interest periods from PM system when available
    select
        cast(null as varchar(64)) as source_id,
        cast(null as varchar(10)) as source_system,
        cast(null as varchar(64)) as loan_id,
        cast(null as date) as period_start,
        cast(null as date) as period_end,
        cast(null as decimal(9,6)) as index_rate_pct,
        cast(null as decimal(9,6)) as margin_pct,
        cast(null as integer) as accrual_days,
        cast(null as numeric(24,2)) as expected_interest_amount,
        cast(null as date) as payment_due_date,
        cast(null as numeric(24,2)) as actual_interest_amount,
        cast(null as timestamp) as created_date,
        cast(null as timestamp) as last_modified_date
    where 1 = 0  -- Return no rows until source data exists
),

all_sources as (
    select * from fund_admin_interest_periods
    union all
    select * from pm_interest_periods
),

-- Resolve loan_id through xref
loan_xref as (
    select
        source_system,
        source_id,
        canonical_id as loan_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'LOAN'
),

interest_periods_resolved as (
    select
        {{ dbt_utils.generate_surrogate_key(['loan_xref.loan_id', 'all_sources.period_start']) }} as loan_interest_period_id,
        loan_xref.loan_id,
        all_sources.period_start,
        all_sources.period_end,
        all_sources.index_rate_pct,
        all_sources.margin_pct,
        all_sources.accrual_days,
        all_sources.expected_interest_amount,
        all_sources.payment_due_date,
        all_sources.actual_interest_amount,
        all_sources.created_date as created_at,
        all_sources.last_modified_date as updated_at
    from all_sources
    inner join loan_xref
        on all_sources.source_system = loan_xref.source_system
        and all_sources.loan_id = loan_xref.source_id
)

select * from interest_periods_resolved
