with source_data as (
    {{ amos_core.staging_from_source('crm', 'crm_opportunities') }}
)

select * from source_data
