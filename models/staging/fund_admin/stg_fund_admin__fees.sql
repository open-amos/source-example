with source_data as (
    {{ staging_from_source('fund_admin', 'admin_fees') }}
)

select * from source_data
