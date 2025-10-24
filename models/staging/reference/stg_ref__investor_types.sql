with source_data as (
    {{ staging_from_source('reference', 'shared_investor_types') }}
)

select * from source_data
