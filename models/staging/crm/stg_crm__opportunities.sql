with source_data as (
    {{ staging_from_source('crm', 'crm_opportunities') }}
)

select * from source_data
