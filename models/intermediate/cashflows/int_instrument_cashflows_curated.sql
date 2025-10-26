-- Build instrument cashflows from transactions or PM sources
-- Maps transaction data to instrument-level cash movements
with transactions as (
    select
        transaction_id,
        transaction_type,
        instrument_id,
        amount,
        currency_code,
        fx_rate,
        amount_converted,
        date,
        reference,
        created_at,
        updated_at
    from {{ ref('int_transactions_classified') }}
    where instrument_id is not null
),

-- Map transaction types to instrument cashflow types
cashflows as (
    select
        {{ dbt_utils.generate_surrogate_key(['transaction_id', 'instrument_id']) }} as instrument_cashflow_id,
        instrument_id,
        case
            when transaction_type = 'INVESTMENT_TRANSACTION' then 'CONTRIBUTION'
            when transaction_type = 'DISTRIBUTION' then 'DISTRIBUTION'
            when transaction_type = 'DIVIDEND' then 'DIVIDEND'
            when transaction_type = 'LOAN_INTEREST_RECEIPT' then 'INTEREST'
            when transaction_type = 'LOAN_FEE_RECEIPT' then 'FEE'
            when transaction_type = 'LOAN_PRINCIPAL_REPAYMENT' then 'PRINCIPAL'
            when transaction_type = 'LOAN_DRAW' then 'DRAW'
            else 'OTHER'
        end as instrument_cashflow_type,
        date,
        amount,
        currency_code,
        fx_rate,
        amount_converted,
        transaction_id,
        reference,
        created_at,
        updated_at
    from transactions
)

select * from cashflows
