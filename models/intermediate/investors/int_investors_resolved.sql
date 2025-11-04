with fund_admin_investors as (
    select
        investor_code as source_id,
        'ADMIN' as source_system,
        investor_name as name,
        investor_legal_name as legal_name,
        investor_type,
        domicile_country_code,
        country_code,
        state_province,
        city,
        contact_person,
        contact_email,
        contact_phone,
        kyc_status,
        aml_status,
        accredited_status,
        tax_id,
        tax_jurisdiction,
        investment_capacity,
        risk_tolerance,
        liquidity_preference,
        esg_requirements,
        created_date,
        last_modified_date
    from {{ ref('stg_fund_admin__investors') }}
),

investor_type_synonyms as (
    select
        synonym,
        normalized_synonym,
        source_system,
        investor_type_id
    from {{ ref('stg_ref__investor_type_synonyms') }}
),

-- Map investor_type to canonical investor_type_id using synonyms
investors_with_type_id as (
    select
        fa.source_id,
        fa.source_system,
        fa.name,
        fa.legal_name,
        syn.investor_type_id,
        fa.domicile_country_code,
        fa.country_code,
        fa.state_province,
        fa.city,
        fa.contact_person,
        fa.contact_email,
        fa.contact_phone,
        fa.kyc_status,
        fa.aml_status,
        fa.accredited_status,
        fa.tax_id,
        fa.tax_jurisdiction,
        fa.investment_capacity,
        fa.risk_tolerance,
        fa.liquidity_preference,
        fa.esg_requirements,
        fa.created_date,
        fa.last_modified_date
    from fund_admin_investors fa
    left join investor_type_synonyms syn
        on lower(trim(fa.investor_type)) = syn.normalized_synonym
        and syn.source_system = 'ADMIN'
),

all_sources as (
    select * from investors_with_type_id
),

xref as (
    select
        source_system,
        source_id,
        canonical_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INVESTOR'
),

resolved as (
    select
        xref.canonical_id as investor_id,
        all_sources.source_system,
        all_sources.source_id,
        all_sources.name,
        all_sources.legal_name,
        all_sources.investor_type_id,
        all_sources.domicile_country_code,
        all_sources.country_code,
        all_sources.state_province,
        all_sources.city,
        all_sources.contact_person,
        all_sources.contact_email,
        all_sources.contact_phone,
        all_sources.kyc_status,
        all_sources.aml_status,
        all_sources.accredited_status,
        all_sources.tax_id,
        all_sources.tax_jurisdiction,
        all_sources.investment_capacity,
        all_sources.risk_tolerance,
        all_sources.liquidity_preference,
        all_sources.esg_requirements,
        all_sources.created_date,
        all_sources.last_modified_date
    from all_sources
    inner join xref
        on all_sources.source_system = xref.source_system
        and all_sources.source_id = xref.source_id
)

select * from resolved
