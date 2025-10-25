with capital_calls as (
    select
        fund_code,
        investor_code,
        call_date,
        commitment_amount,
        commitment_currency,
        created_date,
        last_modified_date
    from {{ ref('stg_fund_admin__capital_calls') }}
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

-- Get commitment_id from int_commitments_curated
commitments as (
    select
        commitment_id,
        fund_id,
        investor_id
    from {{ ref('int_commitments_curated') }}
),

-- Build commitment records with changes over time
commitment_changes as (
    select
        cc.fund_code,
        cc.investor_code,
        cc.call_date as date_from,
        cc.commitment_amount as amount,
        'Active' as status,
        cc.created_date,
        cc.last_modified_date,
        row_number() over (
            partition by cc.fund_code, cc.investor_code 
            order by cc.call_date
        ) as change_sequence
    from capital_calls cc
),

-- Resolve to canonical IDs and commitment_id
commitment_records_resolved as (
    select
        {{ dbt_utils.generate_surrogate_key(['c.commitment_id', 'ch.date_from']) }} as commitment_record_id,
        c.commitment_id,
        ch.date_from,
        ch.amount,
        ch.status,
        ch.created_date as created_at,
        ch.last_modified_date as updated_at
    from commitment_changes ch
    inner join fund_xref fx
        on ch.fund_code = fx.source_id
        and fx.source_system = 'ADMIN'
    inner join investor_xref ix
        on ch.investor_code = ix.source_id
        and ix.source_system = 'ADMIN'
    inner join commitments c
        on fx.fund_id = c.fund_id
        and ix.investor_id = c.investor_id
    where ch.change_sequence = 1  -- Only take the first record (initial commitment)
)

select * from commitment_records_resolved
