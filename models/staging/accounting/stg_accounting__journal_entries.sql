with source_data as (
    {{ amos_core.staging_from_source('accounting', 'acc_journal_entries') }}
)

select * from source_data
