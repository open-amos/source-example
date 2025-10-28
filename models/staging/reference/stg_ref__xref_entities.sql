with source_data as (
    {{ amos_core.staging_from_source('reference', 'xref_entities') }}
)

select * from source_data
