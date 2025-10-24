with source_data as (
    {{ staging_from_source('crm', 'crm_opportunity_countries') }}
)

select * from source_data
