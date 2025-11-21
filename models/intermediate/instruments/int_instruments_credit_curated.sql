with instruments as (
    select * from {{ ref('int_instruments_unified') }}
    where instrument_type = 'CREDIT'
),

credit_instruments as (
    select
        instrument_id,
        facility_id,
        -- Map to instrument_credit_type enum (TERM, REVOLVER, DDTL, BRIDGE, MEZZ)
        -- Derive from tranche_label or security_rank
        case
            when lower(tranche_label) like '%revolver%' or lower(tranche_label) like '%revolving%' then 'REVOLVER'
            when lower(tranche_label) like '%mezz%' or security_rank = 'MEZZANINE' then 'MEZZ'
            when lower(tranche_label) like '%bridge%' then 'BRIDGE'
            when lower(tranche_label) like '%delayed%' or lower(tranche_label) like '%ddtl%' then 'DDTL'
            else 'TERM'
        end as instrument_credit_type,
        tranche_label,
        commitment_amount,
        currency_code,
        fx_rate,
        fx_rate_as_of,
        fx_rate_source,
        -- Calculate converted commitment
        case
            when fx_rate is not null and commitment_amount is not null
            then commitment_amount * fx_rate
            else null
        end as commitment_amount_converted,
        start_date,
        maturity_date,
        interest_index,
        null as index_tenor_days,  -- Can be enriched
        null as fixed_rate_pct,    -- Can be enriched
        spread_bps,
        floor_pct,
        day_count,
        pay_freq_months,
        amortization_type,
        security_rank,
        'ACTIVE' as status,  -- Default or map from source
        current_timestamp as created_at,
        current_timestamp as updated_at
    from instruments
    where facility_id is not null  -- Require facility_id for credit instruments
)

select * from credit_instruments
