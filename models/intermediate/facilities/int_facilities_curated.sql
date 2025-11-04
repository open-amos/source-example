with pm_facilities as (
    select
        pm_facility_id as source_id,
        'PM' as source_system,
        pm_fund_id,
        pm_borrower_company_id,
        facility_type,
        agreement_date,
        effective_date,
        maturity_date,
        currency_code,
        total_commitment,
        purpose,
        _source_system,
        _source_loaded_at
    from {{ ref('stg_pm__facilities') }}
),

xref as (
    select
        source_system,
        source_id,
        canonical_id as facility_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FACILITY'
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

facilities_resolved as (
    select
        xref.facility_id,
        fund_xref.fund_id,
        company_xref.company_id as borrower_company_id,
        pm.facility_type,
        pm.agreement_date,
        pm.effective_date,
        pm.maturity_date,
        pm.currency_code,
        pm.total_commitment,
        pm.purpose,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from pm_facilities pm
    inner join xref
        on pm.source_system = xref.source_system
        and pm.source_id = xref.source_id
    inner join fund_xref
        on pm.source_system = fund_xref.source_system
        and pm.pm_fund_id = fund_xref.source_id
    inner join company_xref
        on pm.source_system = company_xref.source_system
        and pm.pm_borrower_company_id = company_xref.source_id
),

-- Map facility_type to canonical enum
-- Based on canonical DBML: 'TERM_LOAN_B', 'UNITRANCHE', 'REVOLVER', 'DELAYED_DRAWDOWN', 'MEZZANINE', 'RCF', 'BRIDGE'
facilities_typed as (
    select
        facility_id,
        fund_id,
        borrower_company_id,
        case
            when upper(facility_type) in ('TERM_LOAN_B', 'TLB') then 'TERM_LOAN_B'
            when upper(facility_type) = 'UNITRANCHE' then 'UNITRANCHE'
            when upper(facility_type) in ('REVOLVER', 'REVOLVING') then 'REVOLVER'
            when upper(facility_type) in ('DELAYED_DRAWDOWN', 'DDTL') then 'DELAYED_DRAWDOWN'
            when upper(facility_type) in ('MEZZANINE', 'MEZZ') then 'MEZZANINE'
            when upper(facility_type) = 'RCF' then 'RCF'
            when upper(facility_type) = 'BRIDGE' then 'BRIDGE'
            else 'TERM_LOAN_B'  -- Default to TERM_LOAN_B for unmapped types
        end as facility_type,
        agreement_date,
        effective_date,
        maturity_date,
        currency_code,
        total_commitment,
        purpose,
        created_at,
        updated_at
    from facilities_resolved
)

select * from facilities_typed
