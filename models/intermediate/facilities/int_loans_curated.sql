-- Loans curated from PM or fund admin sources
-- Loans are linked to both facilities and instruments
-- Note: Currently no loan source data exists in seeds
-- This model will return empty results until loan data is added

with pm_loans as (
    -- Extract loans from PM system
    select
        loan_id as source_id,
        'PM' as source_system,
        instrument_id,
        facility_id,
        loan_type,
        tranche_label,
        commitment_amount,
        currency_code,
        start_date,
        maturity_date,
        interest_index,
        index_tenor_days,
        fixed_rate_pct,
        spread_bps,
        floor_pct,
        day_count,
        pay_freq_months,
        amortization_type,
        security_rank,
        status,
        created_date,
        last_modified_date
    from {{ ref('stg_pm__loans') }}
),

fund_admin_loans as (
    -- Placeholder: Extract loans from fund admin system when available
    select
        cast(null as varchar(64)) as source_id,
        cast(null as varchar(10)) as source_system,
        cast(null as varchar(64)) as instrument_id,
        cast(null as varchar(64)) as facility_id,
        cast(null as varchar(64)) as loan_type,
        cast(null as varchar(64)) as tranche_label,
        cast(null as numeric(24,2)) as commitment_amount,
        cast(null as varchar(3)) as currency_code,
        cast(null as date) as start_date,
        cast(null as date) as maturity_date,
        cast(null as varchar(64)) as interest_index,
        cast(null as integer) as index_tenor_days,
        cast(null as decimal(7,4)) as fixed_rate_pct,
        cast(null as integer) as spread_bps,
        cast(null as decimal(7,4)) as floor_pct,
        cast(null as varchar(64)) as day_count,
        cast(null as integer) as pay_freq_months,
        cast(null as varchar(64)) as amortization_type,
        cast(null as varchar(64)) as security_rank,
        cast(null as varchar(32)) as status,
        cast(null as timestamp) as created_date,
        cast(null as timestamp) as last_modified_date
    where 1 = 0  -- Return no rows until source data exists
),

all_sources as (
    select * from pm_loans
    union all
    select * from fund_admin_loans
),

-- Resolve loan_id through xref
xref as (
    select
        source_system,
        source_id,
        canonical_id as loan_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'LOAN'
),

-- Resolve instrument_id through xref
instrument_xref as (
    select
        source_system,
        source_id,
        canonical_id as instrument_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'INSTRUMENT'
),

-- Resolve facility_id through xref
facility_xref as (
    select
        source_system,
        source_id,
        canonical_id as facility_id
    from {{ ref('stg_ref__xref_entities') }}
    where entity_type = 'FACILITY'
),

loans_resolved as (
    select
        xref.loan_id,
        instrument_xref.instrument_id,
        facility_xref.facility_id,
        all_sources.loan_type,
        all_sources.tranche_label,
        all_sources.commitment_amount,
        all_sources.currency_code,
        all_sources.start_date,
        all_sources.maturity_date,
        all_sources.interest_index,
        all_sources.index_tenor_days,
        all_sources.fixed_rate_pct,
        all_sources.spread_bps,
        all_sources.floor_pct,
        all_sources.day_count,
        all_sources.pay_freq_months,
        all_sources.amortization_type,
        all_sources.security_rank,
        all_sources.status,
        all_sources.created_date,
        all_sources.last_modified_date
    from all_sources
    inner join xref
        on all_sources.source_system = xref.source_system
        and all_sources.source_id = xref.source_id
    inner join instrument_xref
        on all_sources.source_system = instrument_xref.source_system
        and all_sources.instrument_id = instrument_xref.source_id
    inner join facility_xref
        on all_sources.source_system = facility_xref.source_system
        and all_sources.facility_id = facility_xref.source_id
),

-- Map enums to canonical values
-- loan_type: 'TERM', 'REVOLVER', 'DDTL', 'BRIDGE', 'MEZZ'
-- interest_index: 'SOFR', 'EURIBOR', 'SONIA', 'FED_FUNDS', 'FIXED'
-- day_count: '30E_360', 'ACT_360', 'ACT_365', 'ACT_ACT'
-- amortization_type: 'BULLET', 'STRAIGHT_LINE', 'CUSTOM_SCHEDULE'
-- security_rank: 'SENIOR_SECURED', 'SENIOR_UNSECURED', 'SECOND_LIEN', 'MEZZANINE', 'PIK'
loans_typed as (
    select
        loan_id,
        instrument_id,
        facility_id,
        case
            when upper(loan_type) in ('TERM', 'TERM_LOAN') then 'TERM'
            when upper(loan_type) in ('REVOLVER', 'REVOLVING') then 'REVOLVER'
            when upper(loan_type) in ('DDTL', 'DELAYED_DRAWDOWN') then 'DDTL'
            when upper(loan_type) = 'BRIDGE' then 'BRIDGE'
            when upper(loan_type) in ('MEZZ', 'MEZZANINE') then 'MEZZ'
            else 'TERM'  -- Default to TERM for unmapped types
        end as loan_type,
        tranche_label,
        commitment_amount,
        currency_code,
        start_date,
        maturity_date,
        case
            when upper(interest_index) = 'SOFR' then 'SOFR'
            when upper(interest_index) = 'EURIBOR' then 'EURIBOR'
            when upper(interest_index) = 'SONIA' then 'SONIA'
            when upper(interest_index) in ('FED_FUNDS', 'FED FUNDS') then 'FED_FUNDS'
            when upper(interest_index) = 'FIXED' then 'FIXED'
            else 'SOFR'  -- Default to SOFR for unmapped types
        end as interest_index,
        index_tenor_days,
        fixed_rate_pct,
        spread_bps,
        floor_pct,
        case
            when upper(day_count) in ('30E_360', '30E/360', '30/360') then '30E_360'
            when upper(day_count) in ('ACT_360', 'ACT/360', 'ACTUAL/360') then 'ACT_360'
            when upper(day_count) in ('ACT_365', 'ACT/365', 'ACTUAL/365') then 'ACT_365'
            when upper(day_count) in ('ACT_ACT', 'ACT/ACT', 'ACTUAL/ACTUAL') then 'ACT_ACT'
            else 'ACT_360'  -- Default to ACT_360 for unmapped types
        end as day_count,
        pay_freq_months,
        case
            when upper(amortization_type) = 'BULLET' then 'BULLET'
            when upper(amortization_type) in ('STRAIGHT_LINE', 'STRAIGHT LINE', 'LINEAR') then 'STRAIGHT_LINE'
            when upper(amortization_type) in ('CUSTOM_SCHEDULE', 'CUSTOM') then 'CUSTOM_SCHEDULE'
            else 'BULLET'  -- Default to BULLET for unmapped types
        end as amortization_type,
        case
            when upper(security_rank) in ('SENIOR_SECURED', 'SENIOR SECURED') then 'SENIOR_SECURED'
            when upper(security_rank) in ('SENIOR_UNSECURED', 'SENIOR UNSECURED') then 'SENIOR_UNSECURED'
            when upper(security_rank) in ('SECOND_LIEN', 'SECOND LIEN', '2ND LIEN') then 'SECOND_LIEN'
            when upper(security_rank) = 'MEZZANINE' then 'MEZZANINE'
            when upper(security_rank) = 'PIK' then 'PIK'
            else null  -- security_rank is optional
        end as security_rank,
        status,
        created_date as created_at,
        last_modified_date as updated_at
    from loans_resolved
)

select * from loans_typed
