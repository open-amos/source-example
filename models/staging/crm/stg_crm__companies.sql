with source_data as (
    {{ staging_from_source('crm', 'crm_companies') }}
)

select * from source_data
