with companies_resolved as (
    select
        company_id,
        source_system,
        source_id,
        name,
        legal_name,
        website,
        description,
        currency_code,
        created_date,
        last_modified_date
    from {{ ref('int_companies_resolved') }}
),

-- Aggregate data by company_id with priority: CRM > PM
companies_aggregated as (
    select
        company_id,
        -- Use CRM data as priority, fallback to PM
        max(case when source_system = 'CRM' then name end) as crm_name,
        max(case when source_system = 'PM' then name end) as pm_name,
        max(case when source_system = 'CRM' then legal_name end) as crm_legal_name,
        max(case when source_system = 'CRM' then website end) as crm_website,
        max(case when source_system = 'CRM' then description end) as crm_description,
        max(case when source_system = 'CRM' then currency_code end) as crm_currency_code,
        min(created_date) as earliest_created_date,
        max(last_modified_date) as latest_modified_date
    from companies_resolved
    group by company_id
),

unified as (
    select
        company_id,
        coalesce(crm_name, pm_name) as name,
        crm_legal_name as legal_name,
        crm_website as website,
        crm_description as description,
        crm_currency_code as currency_code,
        earliest_created_date as created_at,
        latest_modified_date as updated_at
    from companies_aggregated
)

select * from unified
