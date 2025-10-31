with instruments_resolved as (
    select
        instrument_id,
        source_system,
        source_id,
        name,
        fund_id,
        company_id,
        investment_type,
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
            else null  -- Fail fast on unmapped types instead of silently defaulting
        end as instrument_type,
        inception_date,
        termination_date,
        description,
        created_date as created_at,
        last_modified_date as updated_at
    from instruments_resolved
),

-- Defense-in-depth deduplication: handle any duplicates that may arise
instruments_deduped as (
    select
        instrument_id,
        fund_id,
        company_id,
        instrument_type,
        inception_date,
        termination_date,
        description,
        created_at,
        updated_at,
        row_number() over (
            partition by instrument_id 
            order by updated_at desc nulls last, created_at desc nulls last
        ) as rn
    from instruments_typed
)

select
    instrument_id,
    fund_id,
    company_id,
    instrument_type,
    inception_date,
    termination_date,
    description,
    created_at,
    updated_at
from instruments_deduped
where rn = 1
