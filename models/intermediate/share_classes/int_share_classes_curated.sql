with instruments as (
    select
        company_id,
        share_class_name
    from {{ ref('int_instruments_unified') }}
    where instrument_type = 'EQUITY'
        and share_class_name is not null
),

share_classes as (
    select
        {{ dbt_utils.generate_surrogate_key(['company_id', 'share_class_name']) }} as share_class_id,
        company_id,
        share_class_name as name,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from instruments
    group by company_id, share_class_name
)

select * from share_classes
