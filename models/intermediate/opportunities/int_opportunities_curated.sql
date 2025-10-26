with crm_opportunities as (
    select
        opportunity_id,
        opportunity_name as name,
        company_id,
        stage as stage_name,
        expected_close_date as close_date,
        amount,
        currency_code,
        source,
        owner_name as responsible,
        description,
        fund_name,
        -- Generate source_id from fund_name by converting to lowercase and replacing spaces with hyphens
        case
            when fund_name is not null and fund_name != '' then
                'CRM-FUND-' || lower(replace(replace(fund_name, ' ', '-'), '''', ''))
            else null
        end as fund_source_id,
        created_date,
        last_modified_date
    from {{ ref('stg_crm__opportunities') }}
),

-- Join to stages to get stage_id
stages as (
    select
        stage_id,
        name as stage_name
    from {{ ref('stg_crm__stages') }}
),

-- Join to companies to get canonical company_id
companies as (
    select
        company_id,
        source_system,
        source_id
    from {{ ref('int_companies_resolved') }}
    where source_system = 'CRM'
),

-- Map fund source_id to canonical fund_id using xref_entities
fund_xref as (
    select
        source_system,
        source_id,
        canonical_id as fund_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FUND'
        and source_system = 'CRM'
),

opportunities_enriched as (
    select
        opp.opportunity_id,
        fx.fund_id,  -- Map fund_source_id to canonical fund_id via xref
        opp.name,
        stg.stage_id,
        comp.company_id,
        opp.responsible,
        opp.amount,
        opp.source,
        opp.close_date,
        opp.created_date as created_at,
        opp.last_modified_date as updated_at
    from crm_opportunities as opp
    left join stages as stg
        on opp.stage_name = stg.stage_name
    left join companies as comp
        on opp.company_id = comp.source_id
    left join fund_xref as fx
        on opp.fund_source_id = fx.source_id
)

select * from opportunities_enriched
