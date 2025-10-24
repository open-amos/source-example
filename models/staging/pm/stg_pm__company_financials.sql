with source_data as (
    {{ staging_from_source('pm', 'pm_company_financials') }}
)

select * from source_data
