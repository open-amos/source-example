with source_data as (
    {{ staging_from_source('reference', 'xref_entities') }}
)

select * from source_data
