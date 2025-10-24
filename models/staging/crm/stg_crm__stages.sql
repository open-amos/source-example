with source_data as (
    {{ staging_from_source('crm', 'crm_stages') }}
)

select * from source_data
