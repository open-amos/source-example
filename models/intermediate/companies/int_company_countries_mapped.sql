with crm_company_countries as (
    select
        crm_company_id,
        country_code,
        primary_flag,
        allocation_pct,
        role
    from {{ ref('stg_crm__company_countries') }}
),

company_xref as (
    select
        source_system,
        source_id,
        canonical_id as company_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'COMPANY'
),

resolved as (
    select
        xref.company_id,
        cc.country_code,
        current_timestamp as valid_from,
        cast(null as timestamp) as valid_to,
        cc.allocation_pct,
        cc.role,
        cc.primary_flag,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from crm_company_countries cc
    inner join company_xref xref
        on cc.crm_company_id = xref.source_id
        and xref.source_system = 'CRM'
)

select * from resolved
