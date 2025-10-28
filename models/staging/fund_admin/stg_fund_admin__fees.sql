with source_data as (
    {{ amos_core.staging_from_source('fund_admin', 'admin_fees') }}
)

select * from source_data
