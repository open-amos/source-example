with instruments_resolved as (
    select
        instrument_id,
        source_system,
        source_id,
        inception_date,
        termination_date
    from {{ ref('int_instruments_resolved') }}
),

pm_investments as (
    select distinct
        investment_code as source_id,
        sector
    from {{ ref('stg_pm__investment_rounds') }}
    where sector is not null
),

industry_synonyms as (
    select
        synonym,
        normalized_synonym,
        source_system,
        industry_id
    from {{ ref('stg_ref__industry_synonyms') }}
),

-- Map sectors from PM investments to industries via synonyms
pm_instrument_industries as (
    select
        ir.instrument_id,
        syn.industry_id,
        -- Use inception_date as valid_from for temporal validity
        ir.inception_date as valid_from,
        -- Use termination_date as valid_to (can be null for ongoing instruments)
        ir.termination_date as valid_to,
        100.0 as allocation_pct,  -- Single industry per instrument, 100% allocation
        true as primary_flag,  -- Mark as primary since it's the only industry
        ir.source_system,
        ir.source_id
    from instruments_resolved ir
    inner join pm_investments pm
        on ir.source_id = pm.source_id
        and ir.source_system = 'PM'
    inner join industry_synonyms syn
        on lower(trim(pm.sector)) = syn.normalized_synonym
        and syn.source_system = 'PM'
),

final as (
    select
        instrument_id,
        industry_id,
        allocation_pct,
        primary_flag,
        valid_from,
        valid_to,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from pm_instrument_industries
)

select * from final
