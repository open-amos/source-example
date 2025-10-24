with source_data as (
    {{ staging_from_source('fund_admin', 'admin_capital_calls') }}
)

select * from source_data
