with crm_opportunity_countries as (
    select
        opportunity_id,
        country_code,
        percentage as allocation_pct,
        loaded_at
    from {{ ref('stg_crm__opportunity_countries') }}
),

-- Determine primary flag (highest allocation percentage per opportunity)
country_rankings as (
    select
        opportunity_id,
        country_code,
        allocation_pct,
        row_number() over (
            partition by opportunity_id 
            order by allocation_pct desc, country_code
        ) as rank_num
    from crm_opportunity_countries
),

opportunity_countries_enriched as (
    select
        opportunity_id,
        country_code,
        case when rank_num = 1 then true else false end as primary_flag,
        allocation_pct,
        'OPERATING' as role,  -- Default role for opportunity countries
        current_timestamp as created_at,
        current_timestamp as updated_at
    from country_rankings
)

select * from opportunity_countries_enriched
