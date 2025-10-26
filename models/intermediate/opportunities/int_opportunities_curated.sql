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

-- Join to funds (opportunities may not always have fund_id in CRM)
-- For now, we'll leave fund_id as null since CRM data doesn't have it
opportunities_enriched as (
    select
        opp.opportunity_id,
        null as fund_id,  -- CRM opportunities don't have fund assignment yet
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
)

select * from opportunities_enriched
