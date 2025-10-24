with instruments_resolved as (
    select
        instrument_id,
        source_system,
        source_id,
        inception_date,
        termination_date
    from {{ ref('int_instruments_resolved') }}
),

pm_investment_countries as (
    select
        investment_id,
        country_code,
        percentage as allocation_pct
    from {{ ref('stg_pm__investment_countries') }}
    where investment_id is not null
        and country_code is not null
),

-- Join investment countries to resolved instruments
instrument_countries as (
    select
        instruments_resolved.instrument_id,
        pm_investment_countries.country_code,
        pm_investment_countries.allocation_pct,
        -- Use inception_date as valid_from for temporal validity
        instruments_resolved.inception_date as valid_from,
        -- Use termination_date as valid_to (can be null for ongoing instruments)
        instruments_resolved.termination_date as valid_to,
        -- Determine primary flag (highest allocation percentage)
        row_number() over (
            partition by instruments_resolved.instrument_id 
            order by pm_investment_countries.allocation_pct desc
        ) = 1 as primary_flag,
        null as role,  -- Role not provided in source data
        current_timestamp as created_at,
        current_timestamp as updated_at
    from instruments_resolved
    inner join pm_investment_countries
        on instruments_resolved.source_system = 'PM'
        and instruments_resolved.source_id = pm_investment_countries.investment_id
)

select * from instrument_countries
