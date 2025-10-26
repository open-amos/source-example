with source_data as (
    {{ staging_from_source('pm', 'pm_investment_rounds') }}
)

select * from source_data
