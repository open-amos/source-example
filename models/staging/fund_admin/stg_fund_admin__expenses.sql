with source_data as (
    {{ amos_core.staging_from_source('fund_admin', 'admin_expenses') }}
)

select * from source_data
