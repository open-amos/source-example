with companies as (
    select
        company_id,
        country_code
    from {{ ref('stg_crm__companies') }}
)

select
    company_id as crm_company_id,
    country_code,
    true as primary_flag,
    100.0 as allocation_pct,
    'DOMICILE' as role
from companies
where country_code is not null


