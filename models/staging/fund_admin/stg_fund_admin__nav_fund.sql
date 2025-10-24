with source_data as (
    {{ staging_from_source('fund_admin', 'admin_nav_fund') }}
)

select * from source_data
