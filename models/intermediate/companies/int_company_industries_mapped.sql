with companies_resolved as (
    select
        company_id,
        source_system,
        source_id
    from {{ ref('int_companies_resolved') }}
),

crm_companies as (
    select
        company_id as source_id,
        industry_primary,
        industry_secondary
    from {{ ref('stg_crm__companies') }}
),

industry_synonyms as (
    select
        synonym,
        normalized_synonym,
        source_system,
        industry_id
    from {{ ref('stg_ref__industry_synonyms') }}
),

-- Map primary industries from CRM
crm_primary_industries as (
    select
        cr.company_id,
        syn.industry_id,
        true as primary_flag,
        cr.source_system,
        cr.source_id
    from companies_resolved cr
    inner join crm_companies crm
        on cr.source_id = crm.source_id
        and cr.source_system = 'CRM'
    inner join industry_synonyms syn
        on lower(trim(crm.industry_primary)) = syn.normalized_synonym
        and syn.source_system = 'CRM'
    where crm.industry_primary is not null
),

-- Map secondary industries from CRM
crm_secondary_industries as (
    select
        cr.company_id,
        syn.industry_id,
        false as primary_flag,
        cr.source_system,
        cr.source_id
    from companies_resolved cr
    inner join crm_companies crm
        on cr.source_id = crm.source_id
        and cr.source_system = 'CRM'
    inner join industry_synonyms syn
        on lower(trim(crm.industry_secondary)) = syn.normalized_synonym
        and syn.source_system = 'CRM'
    where crm.industry_secondary is not null
),

-- Union all industry mappings
all_company_industries as (
    select * from crm_primary_industries
    union all
    select * from crm_secondary_industries
),

-- Deduplicate and ensure only one primary per company
final as (
    select
        company_id,
        industry_id,
        primary_flag,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from all_company_industries
    qualify row_number() over (
        partition by company_id, industry_id 
        order by case when primary_flag then 0 else 1 end
    ) = 1
)

select * from final
