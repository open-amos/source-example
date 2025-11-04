pm_instrument_industries as (
    select
        pm_instrument_id,
        pm_industry_id,
        primary_flag
    from {{ ref('stg_pm__instrument_industries') }}
),

xref_instruments as (
    select
        source_id,
        canonical_id as instrument_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INSTRUMENT'
      and source_system = 'PM'
),

xref_industries as (
    select
        source_id,
        canonical_id as industry_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INDUSTRY'
      and source_system = 'PM'
),

instruments_unified as (
    select
        instrument_id,
        inception_date,
        termination_date
    from {{ ref('int_instruments_unified') }}
),

mapped as (
    select
        xu.instrument_id,
        xi.industry_id,
        cast(null as numeric(5,2)) as allocation_pct,
        pmi.primary_flag,
        xu.inception_date as valid_from,
        xu.termination_date as valid_to,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from pm_instrument_industries pmi
    inner join xref_instruments xinst
        on pmi.pm_instrument_id = xinst.source_id
    inner join instruments_unified xu
        on xinst.instrument_id = xu.instrument_id
    inner join xref_industries xi
        on pmi.pm_industry_id = xi.source_id
)

select * from mapped
