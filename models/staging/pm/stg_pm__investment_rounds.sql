with source_data as (
    {{ amos_core.staging_from_source('pm', 'pm_investment_rounds') }}
)

select * from source_data
