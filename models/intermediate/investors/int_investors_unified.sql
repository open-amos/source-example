with investors_resolved as (
    select
        investor_id,
        source_system,
        source_id,
        name,
        legal_name,
        investor_type_id,
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
    from {{ ref('int_investors_resolved') }}
),

-- Aggregate data by investor_id (currently only ADMIN source, but prepared for future sources)
investors_aggregated as (
    select
        investor_id,
        max(case when source_system = 'ADMIN' then name end) as admin_name,
        max(case when source_system = 'ADMIN' then legal_name end) as admin_legal_name,
        max(case when source_system = 'ADMIN' then investor_type_id end) as admin_investor_type_id,
        max(case when source_system = 'ADMIN' then domicile_country_code end) as admin_domicile_country_code,
        max(case when source_system = 'ADMIN' then country_code end) as admin_country_code,
        max(case when source_system = 'ADMIN' then state_province end) as admin_state_province,
        max(case when source_system = 'ADMIN' then city end) as admin_city,
        max(case when source_system = 'ADMIN' then contact_person end) as admin_contact_person,
        max(case when source_system = 'ADMIN' then contact_email end) as admin_contact_email,
        max(case when source_system = 'ADMIN' then contact_phone end) as admin_contact_phone,
        max(case when source_system = 'ADMIN' then kyc_status end) as admin_kyc_status,
        max(case when source_system = 'ADMIN' then aml_status end) as admin_aml_status,
        max(case when source_system = 'ADMIN' then accredited_status end) as admin_accredited_status,
        max(case when source_system = 'ADMIN' then tax_id end) as admin_tax_id,
        max(case when source_system = 'ADMIN' then tax_jurisdiction end) as admin_tax_jurisdiction,
        max(case when source_system = 'ADMIN' then investment_capacity end) as admin_investment_capacity,
        max(case when source_system = 'ADMIN' then risk_tolerance end) as admin_risk_tolerance,
        max(case when source_system = 'ADMIN' then liquidity_preference end) as admin_liquidity_preference,
        max(case when source_system = 'ADMIN' then esg_requirements end) as admin_esg_requirements,
        min(created_date) as earliest_created_date,
        max(last_modified_date) as latest_modified_date
    from investors_resolved
    group by investor_id
),

unified as (
    select
        investor_id,
        admin_name as name,
        admin_investor_type_id as investor_type_id,
        admin_domicile_country_code as domicile_country_code,
        admin_country_code as country_code,
        admin_state_province as state_province,
        admin_city as city,
        admin_contact_person as contact_person,
        admin_contact_email as contact_email,
        admin_contact_phone as contact_phone,
        admin_kyc_status as kyc_status,
        admin_aml_status as aml_status,
        admin_accredited_status as accredited_status,
        admin_tax_id as tax_id,
        admin_tax_jurisdiction as tax_jurisdiction,
        admin_investment_capacity as investment_capacity,
        admin_risk_tolerance as risk_tolerance,
        admin_liquidity_preference as liquidity_preference,
        admin_esg_requirements as esg_requirements,
        earliest_created_date as created_at,
        latest_modified_date as updated_at
    from investors_aggregated
)

select * from unified
