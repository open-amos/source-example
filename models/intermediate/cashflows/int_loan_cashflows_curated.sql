-- Build loan cashflows from fund admin or PM sources
-- Maps transaction data to loan-level cash movements
-- Note: Currently no loan source data exists in seeds
-- This model will return empty results until loan data is added

with transactions as (
    select
        transaction_id,
        transaction_type,
        facility_id,
        amount,
        currency_code,
        fx_rate,
        amount_converted,
        date,
        reference,
        created_at,
        updated_at
    from {{ ref('int_transactions_classified') }}
    where facility_id is not null
),

-- Get loan_id from facilities (loans are linked to facilities)
loans as (
    select
        loan_id,
        facility_id
    from {{ ref('int_loans_curated') }}
),

-- Map transaction types to loan cashflow types
cashflows_with_loans as (
    select
        {{ dbt_utils.generate_surrogate_key(['t.transaction_id', 'l.loan_id']) }} as loan_cashflow_id,
        l.loan_id,
        case
            when t.transaction_type = 'LOAN_DRAW' then 'DRAW'
            when t.transaction_type = 'LOAN_PRINCIPAL_REPAYMENT' then 'PRINCIPAL_REPAYMENT'
            when t.transaction_type = 'LOAN_INTEREST_RECEIPT' then 'INTEREST_PAYMENT'
            when t.transaction_type = 'LOAN_FEE_RECEIPT' then 'FEE_PAYMENT'
            else 'DRAW'  -- Default for unmapped types
        end as loan_cashflow_type,
        t.date,
        t.amount,
        t.currency_code,
        t.fx_rate,
        t.amount_converted,
        cast(null as varchar(500)) as interest_period_id,  -- Will be linked in marts layer
        t.transaction_id,
        t.reference,
        t.created_at,
        t.updated_at
    from transactions t
    inner join loans l
        on t.facility_id = l.facility_id
)

select * from cashflows_with_loans
