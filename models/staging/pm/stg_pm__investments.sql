with source_data as (
    {{ staging_from_source('pm', 'pm_investments') }}
)

select * from source_data
