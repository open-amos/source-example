with source_data as (
    {{ amos_core.staging_from_source('pm', 'pm_instrument_industries') }}
)

select * from source_data
