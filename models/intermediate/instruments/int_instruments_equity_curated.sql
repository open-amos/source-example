with instruments as (
    select * from {{ ref('int_instruments_unified') }}
    where instrument_type = 'EQUITY'
),

share_classes as (
    select
        company_id,
        share_class_name as name,
        {{ dbt_utils.generate_surrogate_key(['company_id', 'share_class_name']) }} as share_class_id
    from instruments
    where share_class_name is not null
    group by company_id, share_class_name
),

equity_instruments as (
    select
        i.instrument_id,
        sc.share_class_id,
        i.initial_share_count,
        i.initial_ownership_pct,
        i.initial_cost,
        i.initial_price_per_share,
        i.currency_code,
        i.fx_rate,
        i.fx_rate_as_of,
        i.fx_rate_source,
        -- Calculate converted cost
        case
            when i.fx_rate is not null and i.initial_cost is not null
            then i.initial_cost * i.fx_rate
            else null
        end as initial_cost_converted,
        null as description,  -- Can be enriched from other sources
        current_timestamp as created_at,
        current_timestamp as updated_at
    from instruments i
    left join share_classes sc
        on i.company_id = sc.company_id
        and i.share_class_name = sc.name
)

select * from equity_instruments
