with source_data as (
    {{ amos_core.staging_from_source('pm', 'pm_loan_snapshots') }}
)

select * from source_data
