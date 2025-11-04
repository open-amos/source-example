with pm_instrument_countries as (
    select
        pm_instrument_id,
        country_code,
        primary_flag,
        allocation_pct,
        role
    from {{ ref('stg_pm__instrument_countries') }}
),

instrument_xref as (
    select
        source_system,
        source_id,
        canonical_id as instrument_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INSTRUMENT'
),

resolved as (
    select
        xref.instrument_id,
        ic.country_code,
        current_timestamp as valid_from,
        cast(null as timestamp) as valid_to,
        ic.allocation_pct,
        ic.role,
        ic.primary_flag,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from pm_instrument_countries ic
    inner join instrument_xref xref
        on ic.pm_instrument_id = xref.source_id
        and xref.source_system = 'PM'
)

select * from resolved
