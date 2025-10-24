with source_data as (
    {{ staging_from_source('fund_admin', 'admin_distributions') }}
)

select * from source_data
