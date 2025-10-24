with source_data as (
    {{ staging_from_source('reference', 'shared_industries') }}
)

select * from source_data
