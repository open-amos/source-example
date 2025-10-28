with source_data as (
    {{ amos_core.staging_from_source('fund_admin', 'admin_investors') }}
)

select * from source_data
