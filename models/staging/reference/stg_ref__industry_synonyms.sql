with source_data as (
    {{ amos_core.staging_from_source('reference', 'shared_industry_synonyms') }}
)

select * from source_data
