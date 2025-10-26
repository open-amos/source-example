-- Build company valuations from pm_valuations
-- Maps PM valuation data to canonical company valuation structure
with pm_valuations as (
    select
        valuation_id,
        company_id as source_company_id,
        'PM' as source_system,
        valuation_date as period_end_date,
        null as period_start_date,  -- Not provided in source
        'QUARTERLY' as frequency,  -- Assuming quarterly reporting
        'GAAP' as reporting_basis,  -- Assuming GAAP basis
        'INTERNAL' as snapshot_source,
        equity_value as amount,
        'ACTUAL' as valuation_type,  -- Assuming actual valuations
        created_date,
        last_modified_date
    from {{ ref('stg_pm__valuations') }}
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
        {{ dbt_utils.generate_surrogate_key(['company_xref.company_id', 'pm_valuations.period_end_date', 'pm_valuations.valuation_type', 'pm_valuations.reporting_basis', 'pm_valuations.snapshot_source']) }} as company_valuation_id,
        company_xref.company_id,
        pm_valuations.period_start_date,
        pm_valuations.period_end_date,
        pm_valuations.frequency,
        pm_valuations.reporting_basis,
        pm_valuations.snapshot_source,
        pm_valuations.amount,
        pm_valuations.valuation_type,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from pm_valuations
    inner join company_xref
        on pm_valuations.source_system = company_xref.source_system
        and pm_valuations.source_company_id = company_xref.source_id
)

select * from curated
