-- Build company financials from pm_company_financials
-- Maps PM financial statement data to canonical company financials structure
with pm_financials as (
    select
        id as company_performance_snapshot_id,
        company_id as source_company_id,
        'PM' as source_system,
        start_date as period_start_date,
        end_date as period_end_date,
        period as frequency,  -- ANNUAL or QUARTERLY
        accounts_type as reporting_basis,  -- AUDITED or MANAGEMENT
        'INTERNAL' as snapshot_source,
        currency_code,
        revenue,
        cost_of_goods_sold,
        gross_profit,
        operating_expenses,
        ebitda,
        depreciation_amortization,
        ebit,
        net_income,
        cash,
        total_assets,
        total_liabilities,
        equity,
        operating_cash_flow,
        investing_cash_flow,
        financing_cash_flow,
        'PM' as source_system_name,
        null as source_file_ref,  -- Not provided in source
        created_at,
        updated_at
    from {{ ref('stg_pm__company_financials') }}
),

-- Resolve company_id through xref
company_xref as (
    select
        source_system,
        source_id,
        canonical_id as company_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'COMPANY'
),

curated as (
    select
        {{ dbt_utils.generate_surrogate_key(['company_xref.company_id', 'pm_financials.period_end_date', 'pm_financials.reporting_basis', 'pm_financials.snapshot_source']) }} as company_performance_snapshot_id,
        company_xref.company_id,
        pm_financials.period_start_date,
        pm_financials.period_end_date,
        pm_financials.frequency,
        pm_financials.reporting_basis,
        pm_financials.snapshot_source,
        pm_financials.currency_code,
        pm_financials.revenue,
        pm_financials.cost_of_goods_sold,
        pm_financials.gross_profit,
        pm_financials.operating_expenses,
        pm_financials.ebitda,
        pm_financials.depreciation_amortization,
        pm_financials.ebit,
        pm_financials.net_income,
        pm_financials.cash,
        pm_financials.total_assets,
        pm_financials.total_liabilities,
        pm_financials.equity,
        pm_financials.operating_cash_flow,
        pm_financials.investing_cash_flow,
        pm_financials.financing_cash_flow,
        pm_financials.source_system_name as source_system,
        pm_financials.source_file_ref,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from pm_financials
    inner join company_xref
        on pm_financials.source_system = company_xref.source_system
        and pm_financials.source_company_id = company_xref.source_id
)

select * from curated
