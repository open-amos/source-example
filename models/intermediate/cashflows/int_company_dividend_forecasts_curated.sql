-- Build dividend forecasts from PM sources
-- Projected dividend payments for portfolio companies
-- Note: Currently no dividend forecast source data exists in seeds
-- This model will return empty results until dividend forecast data is added

with pm_dividend_forecasts as (
    -- Placeholder: Extract dividend forecasts from PM system when available
    -- Expected columns: company_id, date, amount
    select
        cast(null as varchar(64)) as source_id,
        cast(null as varchar(10)) as source_system,
        cast(null as varchar(64)) as company_id,
        cast(null as date) as date,
        cast(null as numeric(20,2)) as amount,
        cast(null as timestamp) as created_date,
        cast(null as timestamp) as last_modified_date
    where 1 = 0  -- Return no rows until source data exists
),

all_sources as (
    select * from pm_dividend_forecasts
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

dividend_forecasts_resolved as (
    select
        {{ dbt_utils.generate_surrogate_key(['company_xref.company_id', 'all_sources.date']) }} as company_dividend_forecast_id,
        company_xref.company_id,
        all_sources.date,
        all_sources.amount,
        all_sources.created_date as created_at,
        all_sources.last_modified_date as updated_at
    from all_sources
    inner join company_xref
        on all_sources.source_system = company_xref.source_system
        and all_sources.company_id = company_xref.source_id
)

select * from dividend_forecasts_resolved
