with pm_instrument_cashflows as (
    select
        pm_instrument_cashflow_id,
        pm_instrument_id,
        instrument_cashflow_type,
        date,
        currency_code,
        fx_rate,
        fx_rate_as_of,
        fx_rate_source,
        amount,
        pm_transaction_id,
        reference,
        non_cash,
        _source_system,
        _source_loaded_at
    from {{ ref('stg_pm__instrument_cashflows') }}
),

instrument_xref as (
    select
        source_system,
        source_id,
        canonical_id as instrument_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INSTRUMENT'
),

resolved as (
    select
        {{ dbt_utils.generate_surrogate_key(['pm.pm_instrument_cashflow_id']) }} as instrument_cashflow_id,
        xref.instrument_id,
        pm.instrument_cashflow_type,
        pm.date,
        pm.amount,
        pm.currency_code,
        pm.fx_rate,
        pm.fx_rate_as_of,
        pm.fx_rate_source,
        -- Calculate converted amount
        case
            when pm.fx_rate is not null and pm.amount is not null
            then pm.amount * pm.fx_rate
            else null
        end as amount_converted,
        pm.pm_transaction_id as transaction_id,
        pm.reference,
        pm.non_cash,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from pm_instrument_cashflows pm
    inner join instrument_xref xref
        on pm.pm_instrument_id = xref.source_id
        and xref.source_system = 'PM'
)

select * from resolved
