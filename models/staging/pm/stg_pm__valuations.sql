with source_data as (
    {{ staging_from_source('pm', 'pm_valuations') }}
)

select * from source_data
