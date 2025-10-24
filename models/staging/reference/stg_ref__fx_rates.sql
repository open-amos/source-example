with source_data as (
    {{ staging_from_source('reference', 'shared_fx_rates') }}
)

select * from source_data
