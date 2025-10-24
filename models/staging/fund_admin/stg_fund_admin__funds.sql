with source_data as (
    {{ staging_from_source('fund_admin', 'admin_funds') }}
)

select * from source_data
