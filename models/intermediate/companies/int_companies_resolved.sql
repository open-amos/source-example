with crm_companies as (
    select
        company_id as source_id,
        'CRM' as source_system,
        company_name as name,
        legal_name,
        website,
        description,
        base_currency_code as currency_code,
        created_date,
        last_modified_date
    from {{ ref('stg_crm__companies') }}
),

pm_companies as (
    select distinct
        pm_company_id as source_id,
        'PM' as source_system,
        null as name,
        null as legal_name,
        null as website,
        null as description,
        null as currency_code,
        cast(null as date) as created_date,
        cast(null as date) as last_modified_date
    from {{ ref('stg_pm__instruments') }}
    where pm_company_id is not null
),

all_sources as (
    select * from crm_companies
    union all
    select * from pm_companies
),

xref as (
    select
        source_system,
        source_id,
        canonical_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'COMPANY'
),

resolved as (
    select
        xref.canonical_id as company_id,
        all_sources.source_system,
        all_sources.source_id,
        all_sources.name,
        all_sources.legal_name,
        all_sources.website,
        all_sources.description,
        all_sources.currency_code,
        all_sources.created_date,
        all_sources.last_modified_date
    from all_sources
    inner join xref
        on all_sources.source_system = xref.source_system
        and all_sources.source_id = xref.source_id
)

select * from resolved
