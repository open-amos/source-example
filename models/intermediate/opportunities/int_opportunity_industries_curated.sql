with crm_opportunities as (
    select
        opportunity_id,
        industry as industry_name
    from {{ ref('stg_crm__opportunities') }}
    where industry is not null
),

-- Map industry names to canonical industry_id using xref_entities
-- The CRM system stores industry as a string (e.g., "Technology")
-- xref_entities maps this string to the canonical industry_id (e.g., "IND_TECH")
xref_industries as (
    select
        source_id as industry_name,
        canonical_id as industry_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INDUSTRY'
        and source_system = 'CRM'
),

opportunity_industries_mapped as (
    select
        opp.opportunity_id,
        xref.industry_id,
        true as primary_flag,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from crm_opportunities as opp
    inner join xref_industries as xref
        on opp.industry_name = xref.industry_name
)

select * from opportunity_industries_mapped
