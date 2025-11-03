-- Instruments are at the investment_code level (one instrument can have multiple rounds)
-- We take the first round's metadata for instrument-level attributes
with pm_instruments as (
    select
        investment_code as source_id,
        'PM' as source_system,
        investment_name as name,
        fund_id,
        company_id as pm_company_id,
        investment_type,
        min(round_date) as inception_date,  -- First round date
        target_exit_date as termination_date,
        investment_thesis as description,
        created_date,
        last_modified_date
    from {{ ref('stg_pm__investment_rounds') }}
    where investment_code is not null
    group by 
        investment_code,
        investment_name,
        fund_id,
        company_id,
        investment_type,
        target_exit_date,
        investment_thesis,
        created_date,
        last_modified_date
),

-- Loan instruments from the loans table
-- Group by instrument_id since one instrument can have multiple loan tranches
pm_loan_instruments as (
    select
        instrument_id as source_id,
        'PM' as source_system,
        max(tranche_label) as name,  -- Take one tranche label as the name
        null as fund_id,  -- Loans may not have direct fund_id
        borrower_company_id as pm_company_id,
        'LOAN' as investment_type,
        min(start_date) as inception_date,  -- Earliest start date
        max(maturity_date) as termination_date,  -- Latest maturity date
        null as description,
        min(created_date) as created_date,
        max(last_modified_date) as last_modified_date
    from {{ ref('stg_pm__loans') }}
    where instrument_id is not null
    group by
        instrument_id,
        borrower_company_id
),

-- Union all instruments (equity + loans)
all_instruments as (
    select * from pm_instruments
    union all
    select * from pm_loan_instruments
),

xref as (
    select
        source_system,
        source_id,
        canonical_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INSTRUMENT'
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
        all_instruments.source_system,
        all_instruments.source_id,
        all_instruments.name,
        fund_xref.fund_id,
        company_xref.company_id,
        all_instruments.investment_type,
        all_instruments.inception_date,
        all_instruments.termination_date,
        all_instruments.description,
        all_instruments.created_date,
        all_instruments.last_modified_date
    from all_instruments
    inner join xref
        on all_instruments.source_system = xref.source_system
        and all_instruments.source_id = xref.source_id
    inner join company_xref
        on all_instruments.source_system = company_xref.source_system
        and all_instruments.pm_company_id = company_xref.source_id
    left join fund_xref
        on all_instruments.source_system = fund_xref.source_system
        and all_instruments.fund_id = fund_xref.source_id
)

select * from resolved
