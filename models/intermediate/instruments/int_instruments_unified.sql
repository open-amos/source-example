with instruments_resolved as (
    select
        instrument_id,
        source_system,
        source_id,
        name,
        fund_id,
        company_id,
        investment_type,
        currency_code,
        inception_date,
        termination_date,
        description,
        created_date,
        last_modified_date
    from {{ ref('int_instruments_resolved') }}
),

-- Map investment_type to canonical instrument_type enum
-- Based on canonical DBML: 'EQUITY', 'LOAN', 'CONVERTIBLE', 'WARRANT', 'FUND_INTEREST'
instruments_typed as (
    select
        instrument_id,
        fund_id,
        company_id,
        case
            when upper(investment_type) = 'EQUITY' then 'EQUITY'
            when upper(investment_type) = 'LOAN' then 'LOAN'
            when upper(investment_type) = 'CONVERTIBLE' then 'CONVERTIBLE'
            when upper(investment_type) = 'WARRANT' then 'WARRANT'
            when upper(investment_type) = 'FUND_INTEREST' then 'FUND_INTEREST'
            else 'EQUITY'  -- Default to EQUITY for unmapped types
        end as instrument_type,
        currency_code,
        inception_date,
        termination_date,
        description,
        created_date as created_at,
        last_modified_date as updated_at
    from instruments_resolved
)

select * from instruments_typed
