with source_data as (
    {{ amos_core.staging_from_source('reference', 'shared_industries') }}
)

select * from source_data
