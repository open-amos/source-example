with capital_calls as (
    select
        capital_call_id as source_transaction_id,
        'ADMIN' as source_system,
        'CAPITAL_CALL' as source_table,
        fund_code as source_fund_id,
        investor_code as source_investor_id,
        null as source_instrument_id,
        null as source_investment_round_id,
        null as source_facility_id,
        'DRAWDOWN' as transaction_type,
        'Capital Call #' || cast(call_number as varchar) as name,
        purpose as description,
        call_amount as amount,
        call_currency as currency_code,
        call_date as date,
        'ADMIN' as source,
        capital_call_id as reference,
        created_date,
        last_modified_date
    from {{ ref('stg_fund_admin__capital_calls') }}
),

distributions as (
    select
        distribution_id as source_transaction_id,
        'ADMIN' as source_system,
        'DISTRIBUTION' as source_table,
        fund_code as source_fund_id,
        investor_code as source_investor_id,
        source_investment as source_instrument_id,
        null as source_investment_round_id,
        null as source_facility_id,
        case
            when lower(distribution_type) like '%dividend%' then 'DIVIDEND'
            else 'DISTRIBUTION'
        end as transaction_type,
        'Distribution #' || cast(distribution_number as varchar) as name,
        distribution_type as description,
        distribution_amount as amount,
        distribution_currency as currency_code,
        distribution_date as date,
        'ADMIN' as source,
        distribution_id as reference,
        created_date,
        last_modified_date
    from {{ ref('stg_fund_admin__distributions') }}
),

expenses as (
    select
        expense_id as source_transaction_id,
        'ADMIN' as source_system,
        'EXPENSE' as source_table,
        fund_code as source_fund_id,
        null as source_investor_id,
        allocated_to_investments as source_instrument_id,
        null as source_investment_round_id,
        null as source_facility_id,
        'EXPENSE' as transaction_type,
        expense_type as name,
        description,
        expense_amount as amount,
        expense_currency as currency_code,
        expense_date as date,
        'ADMIN' as source,
        invoice_number as reference,
        created_date,
        last_modified_date
    from {{ ref('stg_fund_admin__expenses') }}
),

fees as (
    select
        fee_id as source_transaction_id,
        'ADMIN' as source_system,
        'FEE' as source_table,
        fund_code as source_fund_id,
        null as source_investor_id,
        null as source_instrument_id,
        null as source_investment_round_id,
        null as source_facility_id,
        'MANAGEMENT_FEE' as transaction_type,
        fee_type as name,
        'Fee period: ' || cast(fee_period_start as varchar) || ' to ' || cast(fee_period_end as varchar) as description,
        fee_amount as amount,
        fee_currency as currency_code,
        fee_calculation_date as date,
        'ADMIN' as source,
        fee_id as reference,
        created_date,
        last_modified_date
    from {{ ref('stg_fund_admin__fees') }}
),

all_transactions as (
    select * from capital_calls
    union all
    select * from distributions
    union all
    select * from expenses
    union all
    select * from fees
),

-- Resolve fund_id using xref
fund_xref as (
    select
        source_system,
        source_id,
        canonical_id as fund_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FUND'
),

-- Resolve investor_id using xref
investor_xref as (
    select
        source_system,
        source_id,
        canonical_id as investor_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INVESTOR'
),

-- Resolve instrument_id using xref (for distributions with source_investment)
instrument_xref as (
    select
        source_system,
        source_id,
        canonical_id as instrument_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INSTRUMENT'
),

-- Resolve investment_round_id using xref
-- Get fund base currencies for FX conversion
fund_base_currencies as (
    select
        fund_id,
        base_currency_code
    from {{ ref('int_funds_unified') }}
),

-- Get FX rates
fx_rates as (
    select
        rate_date,
        base_currency,
        quote_currency,
        exchange_rate,
        rate_source
    from {{ ref('stg_ref__fx_rates') }}
),

transactions_with_resolved_ids as (
    select
        {{ dbt_utils.generate_surrogate_key(['t.source_system', 't.source_transaction_id']) }} as transaction_id,
        t.transaction_type,
        f.fund_id,
        i.investor_id,
        inst.instrument_id,
        null as facility_id,
        t.name,
        t.description,
        t.amount,
        t.currency_code,
        fx.exchange_rate as fx_rate,
        case
            when fx.exchange_rate is not null then t.amount * fx.exchange_rate
            else null
        end as amount_converted,
        fx.rate_date as fx_rate_as_of,
        fx.rate_source as fx_rate_source,
        t.date,
        t.source,
        t.reference,
        t.source_transaction_id as source_reference,
        t.created_date as created_at,
        t.last_modified_date as updated_at
    from all_transactions t
    left join fund_xref f
        on t.source_system = f.source_system
        and t.source_fund_id = f.source_id
    left join investor_xref i
        on t.source_system = i.source_system
        and t.source_investor_id = i.source_id
    left join instrument_xref inst
        on t.source_system = inst.source_system
        and t.source_instrument_id = inst.source_id
    left join fund_base_currencies fbc
        on f.fund_id = fbc.fund_id
    left join fx_rates fx
        on t.currency_code = fx.quote_currency
        and fbc.base_currency_code = fx.base_currency
        and t.date = fx.rate_date
)

select * from transactions_with_resolved_ids
