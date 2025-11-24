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
        non_cash
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

-- Map source cashflow types to canonical taxonomy
cashflow_type_mapping as (
    select
        pm.*,
        case
            -- Equity outflows (investments)
            when lower(pm.instrument_cashflow_type) in ('invest', 'investment', 'contribution', 'follow_on') 
                then 'INVESTMENT'
            
            -- Equity inflows (returns)
            when lower(pm.instrument_cashflow_type) in ('dividend', 'dividends') 
                then 'DIVIDEND'
            when lower(pm.instrument_cashflow_type) in ('exit', 'sale', 'exit_proceeds') 
                then 'EXIT_PROCEEDS'
            when lower(pm.instrument_cashflow_type) in ('distribution_roc', 'roc', 'return_of_capital') 
                then 'RETURN_OF_CAPITAL'
            
            -- Credit outflows
            when lower(pm.instrument_cashflow_type) in ('draw', 'drawdown') 
                then 'DRAW'
            
            -- Credit inflows
            when lower(pm.instrument_cashflow_type) in ('principal', 'principal_repayment') 
                then 'PRINCIPAL'
            when lower(pm.instrument_cashflow_type) in ('interest', 'interest_payment') 
                then 'INTEREST'
            when lower(pm.instrument_cashflow_type) in ('prepayment', 'early_repayment') 
                then 'PREPAYMENT'
            
            -- Shared types
            when lower(pm.instrument_cashflow_type) in ('fee', 'fees') 
                then 'FEE'
            
            -- Fallback
            else 'OTHER'
        end as canonical_cashflow_type,
        
        -- Flag unmapped types for data quality monitoring
        case 
            when lower(pm.instrument_cashflow_type) not in (
                'invest', 'investment', 'contribution', 'follow_on',
                'dividend', 'dividends', 'exit', 'sale', 'exit_proceeds',
                'distribution_roc', 'roc', 'return_of_capital',
                'draw', 'drawdown', 'principal', 'principal_repayment',
                'interest', 'interest_payment', 'prepayment', 'early_repayment',
                'fee', 'fees'
            ) then true 
            else false 
        end as unmapped_type_flag
    from pm_instrument_cashflows pm
),

resolved as (
    select
        {{ dbt_utils.generate_surrogate_key(['pm.pm_instrument_cashflow_id']) }} as instrument_cashflow_id,
        xref.instrument_id,
        pm.canonical_cashflow_type as instrument_cashflow_type,
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
        pm.unmapped_type_flag,
        current_timestamp as created_at,
        current_timestamp as updated_at
    from cashflow_type_mapping pm
    inner join instrument_xref xref
        on pm.pm_instrument_id = xref.source_id
        and xref.source_system = 'PM'
)

select * from resolved
