with crm_opportunities as (
    select
        opportunity_id,
        industry as industry_name
    from {{ ref('stg_crm__opportunities') }}
    where industry is not null
),

-- Map industry names to canonical industry_id using industry synonyms
industry_synonyms as (
    select
        synonym,
        normalized_synonym,
        source_system,
        industry_id
    from {{ ref('stg_ref__industry_synonyms') }}
    where source_system = 'CRM'
),

opportunity_industries_mapped as (
    select
        opp.opportunity_id,
        syn.industry_id,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from crm_opportunities as opp
    inner join industry_synonyms as syn
        on lower(trim(opp.industry_name)) = lower(syn.normalized_synonym)
)

select * from opportunity_industries_mapped
