with pm_instruments as (
    select
        investment_code as source_id,
        'PM' as source_system,
        investment_name as name,
        fund_id,
        company_id as pm_company_id,
        investment_type,
        investment_date as inception_date,
        target_exit_date as termination_date,
        initial_investment_currency as currency_code,
        investment_thesis as description,
        created_date,
        last_modified_date
    from {{ ref('stg_pm__investments') }}
    where investment_code is not null
),

xref as (
    select
        source_system,
        source_id,
        canonical_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INVESTMENT'
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

-- Resolve fund_id through xref
fund_xref as (
    select
        source_system,
        source_id,
        canonical_id as fund_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FUND'
),

resolved as (
    select
        xref.canonical_id as instrument_id,
        pm_instruments.source_system,
        pm_instruments.source_id,
        pm_instruments.name,
        fund_xref.fund_id,
        company_xref.company_id,
        pm_instruments.investment_type,
        pm_instruments.currency_code,
        pm_instruments.inception_date,
        pm_instruments.termination_date,
        pm_instruments.description,
        pm_instruments.created_date,
        pm_instruments.last_modified_date
    from pm_instruments
    inner join xref
        on pm_instruments.source_system = xref.source_system
        and pm_instruments.source_id = xref.source_id
    inner join company_xref
        on pm_instruments.source_system = company_xref.source_system
        and pm_instruments.pm_company_id = company_xref.source_id
    inner join fund_xref
        on pm_instruments.source_system = fund_xref.source_system
        and pm_instruments.fund_id = fund_xref.source_id
)

select * from resolved
