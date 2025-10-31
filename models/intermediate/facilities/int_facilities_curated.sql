-- Facilities curated from PM or fund admin sources
-- Note: Currently no facility source data exists in seeds
-- This model will return empty results until facility data is added

with pm_facilities as (
    -- Extract facilities from PM system
    select
        facility_id as source_id,
        'PM' as source_system,
        cast('FUND-004' as varchar(64)) as fund_id,  -- All facilities belong to Infrastructure Debt Fund
        borrower_company_id,
        facility_type,
        cast(null as varchar(64)) as agent_counterparty_id,
        start_date as agreement_date,
        start_date as effective_date,
        maturity_date,
        currency_code,
        total_commitment,
        cast(null as varchar(256)) as purpose,
        created_date,
        last_modified_date
    from {{ ref('stg_pm__facilities') }}
),

fund_admin_facilities as (
    -- Placeholder: Extract facilities from fund admin system when available
    select
        cast(null as varchar(64)) as source_id,
        cast(null as varchar(10)) as source_system,
        cast(null as varchar(64)) as fund_id,
        cast(null as varchar(64)) as borrower_company_id,
        cast(null as varchar(64)) as facility_type,
        cast(null as varchar(64)) as agent_counterparty_id,
        cast(null as date) as agreement_date,
        cast(null as date) as effective_date,
        cast(null as date) as maturity_date,
        cast(null as varchar(3)) as currency_code,
        cast(null as numeric(24,2)) as total_commitment,
        cast(null as varchar(256)) as purpose,
        cast(null as timestamp) as created_date,
        cast(null as timestamp) as last_modified_date
    where 1 = 0  -- Return no rows until source data exists
),

all_sources as (
    select * from pm_facilities
    union all
    select * from fund_admin_facilities
),

-- Resolve facility_id through xref
xref as (
    select
        source_system,
        source_id,
        canonical_id as facility_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FACILITY'
),

-- Resolve fund_id through xref
fund_xref as (
    select
        source_system,
        source_id,
        canonical_id as fund_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FUND'
),

-- Resolve borrower_company_id through xref
company_xref as (
    select
        source_system,
        source_id,
        canonical_id as company_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'COMPANY'
),

-- Resolve agent_counterparty_id through xref
counterparty_xref as (
    select
        source_system,
        source_id,
        canonical_id as counterparty_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'COUNTERPARTY'
),

facilities_resolved as (
    select
        xref.facility_id,
        fund_xref.fund_id,
        company_xref.company_id as borrower_company_id,
        all_sources.facility_type,
        counterparty_xref.counterparty_id as agent_counterparty_id,
        all_sources.agreement_date,
        all_sources.effective_date,
        all_sources.maturity_date,
        all_sources.currency_code,
        all_sources.total_commitment,
        all_sources.purpose,
        all_sources.created_date,
        all_sources.last_modified_date
    from all_sources
    inner join xref
        on all_sources.source_system = xref.source_system
        and all_sources.source_id = xref.source_id
    inner join fund_xref
        on all_sources.source_system = fund_xref.source_system
        and all_sources.fund_id = fund_xref.source_id
    inner join company_xref
        on all_sources.source_system = company_xref.source_system
        and all_sources.borrower_company_id = company_xref.source_id
    left join counterparty_xref
        on all_sources.source_system = counterparty_xref.source_system
        and all_sources.agent_counterparty_id = counterparty_xref.source_id
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
        agent_counterparty_id,
        agreement_date,
        effective_date,
        maturity_date,
        currency_code,
        total_commitment,
        purpose,
        created_date as created_at,
        last_modified_date as updated_at
    from facilities_resolved
)

select * from facilities_typed
