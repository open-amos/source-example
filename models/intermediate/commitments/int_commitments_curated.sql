with capital_calls as (
    select
        fund_code,
        investor_code,
        commitment_amount,
        commitment_currency,
        min(call_date) as first_call_date,
        min(created_date) as created_date,
        max(last_modified_date) as last_modified_date
    from {{ ref('stg_fund_admin__capital_calls') }}
    group by
        fund_code,
        investor_code,
        commitment_amount,
        commitment_currency
),

-- Resolve fund_id from xref
fund_xref as (
    select
        source_system,
        source_id,
        canonical_id as fund_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FUND'
),

-- Resolve investor_id from xref
investor_xref as (
    select
        source_system,
        source_id,
        canonical_id as investor_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INVESTOR'
),

commitments_resolved as (
    select
        {{ dbt_utils.generate_surrogate_key(['fx.fund_id', 'ix.investor_id']) }} as commitment_id,
        fx.fund_id,
        ix.investor_id,
        cc.created_date as created_at,
        cc.last_modified_date as updated_at
    from capital_calls cc
    inner join fund_xref fx
        on cc.fund_code = fx.source_id
        and fx.source_system = 'ADMIN'
    inner join investor_xref ix
        on cc.investor_code = ix.source_id
        and ix.source_system = 'ADMIN'
)

select * from commitments_resolved
