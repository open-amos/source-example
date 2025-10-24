with source_data as (
    {{ staging_from_source('accounting', 'acc_journal_entries') }}
)

select * from source_data
