with capital_calls as (
    select
        fund_code,
        investor_code,
        call_date,
        call_amount,
        commitment_amount,
        commitment_currency,
        source_reference
    from {{ ref('stg_fund_admin__capital_calls') }}
    where status = 'Paid'
),

distributions as (
    select
        fund_code,
        investor_code,
        distribution_date,
        distribution_amount
    from {{ ref('stg_fund_admin__distributions') }}
    where status = 'Paid'
),

-- Get all unique valuation dates from fund NAV snapshots
valuation_dates as (
    select distinct
        valuation_date as period_end_date
    from {{ ref('stg_fund_admin__nav_fund') }}
),

-- Get commitment_id from int_commitments_curated
commitments as (
    select
        commitment_id,
        fund_id,
        investor_id
    from {{ ref('int_commitments_curated') }}
),

-- Resolve fund_id from xref
fund_xref as (
    select
        source_system,
        source_id,
        canonical_id as fund_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FUND'
),

-- Resolve investor_id from xref
investor_xref as (
    select
        source_system,
        source_id,
        canonical_id as investor_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INVESTOR'
),

-- Calculate cumulative drawdowns by commitment and date
cumulative_drawdowns as (
    select
        cc.fund_code,
        cc.investor_code,
        vd.period_end_date,
        max(cc.commitment_amount) as committed_capital,
        max(cc.commitment_currency) as currency_code,
        sum(cc.call_amount) as called_capital,
        max(cc.source_reference) as source_reference
    from capital_calls cc
    cross join valuation_dates vd
    where cc.call_date <= vd.period_end_date
    group by
        cc.fund_code,
        cc.investor_code,
        vd.period_end_date
),

-- Calculate cumulative distributions by commitment and date
cumulative_distributions as (
    select
        d.fund_code,
        d.investor_code,
        vd.period_end_date,
        sum(d.distribution_amount) as total_distributions
    from distributions d
    cross join valuation_dates vd
    where d.distribution_date <= vd.period_end_date
    group by
        d.fund_code,
        d.investor_code,
        vd.period_end_date
),

-- Combine drawdowns and distributions
commitment_metrics as (
    select
        cd.fund_code,
        cd.investor_code,
        cd.period_end_date,
        cd.committed_capital,
        cd.currency_code,
        cd.called_capital,
        coalesce(cdi.total_distributions, 0) as total_distributions,
        cd.source_reference
    from cumulative_drawdowns cd
    left join cumulative_distributions cdi
        on cd.fund_code = cdi.fund_code
        and cd.investor_code = cdi.investor_code
        and cd.period_end_date = cdi.period_end_date
),

-- Resolve to canonical IDs
commitment_snapshots_resolved as (
    select
        {{ dbt_utils.generate_surrogate_key(['c.commitment_id', 'cm.period_end_date']) }} as commitment_snapshot_id,
        c.commitment_id,
        cast({{ amos_core.compat_dateadd('month', -3, 'cm.period_end_date') }} as date) as period_start_date,
        cm.period_end_date,
        'QUARTERLY' as frequency,
        'GAAP' as reporting_basis,
        'ADMIN' as snapshot_source,
        cm.currency_code,
        'ADMIN' as source,
        cm.source_reference,
        cm.committed_capital,
        cm.called_capital,
        cm.total_distributions,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from commitment_metrics cm
    inner join fund_xref fx
        on cm.fund_code = fx.source_id
        and fx.source_system = 'ADMIN'
    inner join investor_xref ix
        on cm.investor_code = ix.source_id
        and ix.source_system = 'ADMIN'
    inner join commitments c
        on fx.fund_id = c.fund_id
        and ix.investor_id = c.investor_id
)

select * from commitment_snapshots_resolved
