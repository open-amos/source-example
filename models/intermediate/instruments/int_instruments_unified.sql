with pm_instruments as (
    select
        pm_instrument_id as source_id,
        'PM' as source_system,
        pm_fund_id,
        pm_company_id,
        instrument_name as name,
        instrument_type,
        inception_date,
        termination_date,
        description,
        
        -- Equity-specific fields
        share_class_name,
        initial_share_count,
        initial_ownership_pct,
        initial_cost,
        initial_price_per_share,
        
        -- Credit-specific fields
        pm_facility_id,
        tranche_label,
        commitment_amount,
        start_date,
        maturity_date,
        interest_index,
        spread_bps,
        floor_pct,
        day_count,
        pay_freq_months,
        amortization_type,
        security_rank,
        
        -- FX fields
        currency_code,
        fx_rate,
        fx_rate_as_of,
        fx_rate_source
    from {{ ref('stg_pm__instruments') }}
),

xref as (
    select
        source_system,
        source_id,
        canonical_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INSTRUMENT'
),

fund_xref as (
    select
        source_system,
        source_id,
        canonical_id as fund_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FUND'
),

company_xref as (
    select
        source_system,
        source_id,
        canonical_id as company_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'COMPANY'
),

facility_xref as (
    select
        source_system,
        source_id,
        canonical_id as facility_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FACILITY'
),

resolved as (
    select
        xref.canonical_id as instrument_id,
        pm.source_system,
        pm.source_id,
        fund_xref.fund_id,
        company_xref.company_id,
        pm.name,
        pm.instrument_type,
        pm.inception_date,
        pm.termination_date,
        pm.description,
        
        -- Equity fields
        pm.share_class_name,
        pm.initial_share_count,
        pm.initial_ownership_pct,
        pm.initial_cost,
        pm.initial_price_per_share,
        
        -- Credit fields
        facility_xref.facility_id,
        pm.tranche_label,
        pm.commitment_amount,
        pm.start_date,
        pm.maturity_date,
        pm.interest_index,
        pm.spread_bps,
        pm.floor_pct,
        pm.day_count,
        pm.pay_freq_months,
        pm.amortization_type,
        pm.security_rank,
        
        -- FX fields
        pm.currency_code,
        pm.fx_rate,
        pm.fx_rate_as_of,
        pm.fx_rate_source,
        
        current_timestamp as created_at,
        current_timestamp as updated_at
    from pm_instruments pm
    inner join xref
        on pm.source_system = xref.source_system
        and pm.source_id = xref.source_id
    inner join fund_xref
        on pm.source_system = fund_xref.source_system
        and pm.pm_fund_id = fund_xref.source_id
    inner join company_xref
        on pm.source_system = company_xref.source_system
        and pm.pm_company_id = company_xref.source_id
    left join facility_xref
        on pm.source_system = facility_xref.source_system
        and pm.pm_facility_id = facility_xref.source_id
)

select * from resolved
