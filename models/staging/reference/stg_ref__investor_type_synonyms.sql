with source_data as (
    {{ staging_from_source('reference', 'shared_investor_type_synonyms') }}
)

select * from source_data
