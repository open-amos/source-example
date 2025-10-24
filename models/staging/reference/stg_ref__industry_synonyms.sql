with source_data as (
    {{ staging_from_source('reference', 'shared_industry_synonyms') }}
)

select * from source_data
