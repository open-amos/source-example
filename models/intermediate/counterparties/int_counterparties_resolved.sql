with pm_counterparties as (
    -- Placeholder: Extract counterparties from PM system when available
    -- Expected columns: counterparty_id, name, type, country_code
    select
        cast(null as varchar(64)) as source_id,
        cast(null as varchar(10)) as source_system,
        cast(null as varchar(255)) as name,
        cast(null as varchar(64)) as type,
        cast(null as char(2)) as country_code,
        cast(null as timestamp) as created_date,
        cast(null as timestamp) as last_modified_date
    where 1 = 0  -- Return no rows (placeholder)
),

fund_admin_counterparties as (
    -- Placeholder: Extract counterparties from fund admin system when available
    -- Expected columns: counterparty_id, name, type, country_code
    select
        cast(null as varchar(64)) as source_id,
        cast(null as varchar(10)) as source_system,
        cast(null as varchar(255)) as name,
        cast(null as varchar(64)) as type,
        cast(null as char(2)) as country_code,
        cast(null as timestamp) as created_date,
        cast(null as timestamp) as last_modified_date
    where 1 = 0  -- Return no rows (placeholder)
),

all_sources as (
    select * from pm_counterparties
    union all
    select * from fund_admin_counterparties
),

xref as (
    select
        source_system,
        source_id,
        canonical_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'COUNTERPARTY'
),

resolved as (
    select
        xref.canonical_id as counterparty_id,
        all_sources.source_system,
        all_sources.source_id,
        all_sources.name,
        all_sources.type,
        all_sources.country_code,
        all_sources.created_date,
        all_sources.last_modified_date
    from all_sources
    inner join xref
        on all_sources.source_system = xref.source_system
        and all_sources.source_id = xref.source_id
)

select * from resolved
