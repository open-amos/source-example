-- Build loan cashflows from fund admin or PM sources
-- Maps cashflow data to loan-level cash movements

with pm_cashflows as (
    -- Extract loan cashflows from PM system
    select
        loan_cashflow_id,
        loan_id,
        cashflow_type as loan_cashflow_type,
        cashflow_date as date,
        amount,
        currency_code,
        cast(1.0 as decimal(18,6)) as fx_rate,  -- Assume no FX conversion for now
        amount as amount_converted,
        cast(null as varchar(500)) as interest_period_id,
        cast(null as varchar(64)) as transaction_id,
        description as reference,
        created_date as created_at,
        last_modified_date as updated_at
    from {{ ref('stg_pm__loan_cashflows') }}
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

cashflows_resolved as (
    select
        cf.loan_cashflow_id,
        xref.loan_id,
        cf.loan_cashflow_type,
        cf.date,
        cf.amount,
        cf.currency_code,
        cf.fx_rate,
        cf.amount_converted,
        cf.interest_period_id,
        cf.transaction_id,
        cf.reference,
        cf.created_at,
        cf.updated_at
    from pm_cashflows cf
    inner join loan_xref xref
        on 'PM' = xref.source_system
        and cf.loan_id = xref.source_id
)

select * from cashflows_resolved
