with source_data as (
    {{ amos_core.staging_from_source('crm', 'crm_companies') }}
)

select * from source_data
